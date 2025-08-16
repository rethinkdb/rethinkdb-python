#!/usr/bin/env python

# Copyright 2016-2025 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.

"""
`rethinkdb import` loads data into a RethinkDB cluster
"""

import base64
import codecs
import collections
import csv
import json
import logging
import multiprocessing
import os
import shutil
import signal
import sys
import tarfile
import tempfile
import threading
import time
import traceback
from typing import Optional

import click

import rethinkdb as r
from rethinkdb.cli.utils import (
    common_options,
    get_connection,
    get_logger,
    parse_list_args,
    print_progress,
)

# Constants
JSON_READ_CHUNK_SIZE = 128 * 1024
JSON_MAX_BUFFER_SIZE = 128 * 1024 * 1024
MAX_NESTING_DEPTH = 100

Error = collections.namedtuple("Error", ["message", "traceback", "file"])


class SourceFile:
    """Base class for source files that can be imported."""

    format: Optional[str] = None  # set by subclasses
    name: Optional[str] = None
    db = None
    table = None
    primary_key = None
    indexes = None
    write_hook = None
    source_options = None
    start_time = None
    end_time = None
    query_runner = None
    _source = None  # open filehandle for the source

    # Internal synchronization variables
    _bytes_size = None
    _bytes_read = None  # -1 until started
    _total_rows = None  # -1 until known
    _rows_read = None
    _rows_written = None

    def __init__(
        self,
        source,
        db,
        table,
        query_runner,
        primary_key=None,
        indexes=None,
        write_hook=None,
        source_options=None,
    ):
        if self.format is None:
            raise AssertionError(f"{self.__class__.__name__} must have a format")

        if db == "rethinkdb":
            raise AssertionError("Cannot import tables into the system database")

        # query_runner
        self.query_runner = query_runner

        # Reporting information
        self._bytes_size = multiprocessing.Value("i", -1)
        self._bytes_read = multiprocessing.Value("i", -1)
        self._total_rows = multiprocessing.Value("i", -1)
        self._rows_read = multiprocessing.Value("i", 0)
        self._rows_written = multiprocessing.Value("i", 0)

        # source
        if hasattr(source, "read"):
            if hasattr(source, "mode") and "b" in source.mode:
                # Binary file, assume utf-8 encoding
                self._source = codecs.getreader("utf-8")(source)
            else:
                # Assume it has the right encoding
                self._source = source
        else:
            try:
                self._source = codecs.open(source, mode="r", encoding="utf-8")
            except IOError as exc:
                raise ValueError(f'Unable to open source file "{source}": {exc}')

        if (
            hasattr(self._source, "name")
            and self._source.name
            and os.path.isfile(self._source.name)
        ):
            self._bytes_size.value = os.path.getsize(source)
            if self._bytes_size.value == 0:
                raise ValueError(f"Source is zero-length: {source}")

        # Table info
        self.db = db
        self.table = table
        self.primary_key = primary_key
        self.indexes = indexes or {}
        self.write_hook = write_hook or []

        # Options
        self.source_options = source_options or {
            "create_args": {"primary_key": self.primary_key}
        }

        # Name
        if hasattr(self._source, "name") and self._source.name:
            self.name = os.path.basename(self._source.name)
        else:
            self.name = f"{self.db}.{self.table}"

    def __hash__(self):
        return hash((self.db, self.table))

    def get_line(self):
        """Returns a single line from the file"""
        raise NotImplementedError(
            f"This needs to be implemented on the {self.format} subclass"
        )

    # Bytes properties
    @property
    def bytes_size(self):
        return self._bytes_size.value

    @bytes_size.setter
    def bytes_size(self, value):
        self._bytes_size.value = value

    @property
    def bytes_read(self):
        return self._bytes_read.value

    @bytes_read.setter
    def bytes_read(self, value):
        self._bytes_read.value = value

    # Rows properties
    @property
    def total_rows(self):
        return self._total_rows.value

    @total_rows.setter
    def total_rows(self, value):
        self._total_rows.value = value

    @property
    def rows_read(self):
        return self._rows_read.value

    @rows_read.setter
    def rows_read(self, value):
        self._rows_read.value = value

    @property
    def rows_written(self):
        return self._rows_written.value

    def add_rows_written(self, increment):
        with self._rows_written.get_lock():
            self._rows_written.value += increment

    # Percent done
    @property
    def percent_done(self):
        """Return a float between 0 and 1 for a reasonable guess of percentage complete"""
        # Assume that reading takes 50% of the time and writing the other 50%
        completed = 0.0  # of 2.0

        # Add read percentage
        if (
            self._bytes_size.value <= 0
            or self._bytes_size.value <= self._bytes_read.value
        ):
            completed += 1.0
        elif self._bytes_read.value < 0 and self._total_rows.value >= 0:
            # Done by rows read
            if self._rows_read.value > 0:
                completed += float(self._rows_read.value) / float(
                    self._total_rows.value
                )
        else:
            # Done by bytes read
            if self._bytes_read.value > 0:
                completed += float(self._bytes_read.value) / float(
                    self._bytes_size.value
                )

        # Add written percentage
        if self._rows_read.value or self._rows_written.value:
            total_rows = float(self._total_rows.value)
            if total_rows == 0:
                completed += 1.0
            elif total_rows < 0:
                # A guesstimate
                per_row_size = float(self._bytes_read.value) / float(
                    self._rows_read.value
                )
                total_rows = float(self._rows_read.value) + (
                    float(self._bytes_size.value - self._bytes_read.value)
                    / per_row_size
                )
                completed += float(self._rows_written.value) / total_rows
            else:
                # Accurate count
                completed += float(self._rows_written.value) / total_rows

        # Return the value
        return completed * 0.5

    def setup_table(self):
        """Ensure that the db, table, and indexes exist and are correct"""
        logger = get_logger()
        logger.debug(
            f"Setting up table: db={self.db}, table={self.table}, primary_key={self.primary_key}, indexes={list(self.indexes.keys()) if self.indexes else []}"
        )

        # Ensure the table exists and is ready
        logger.debug("Creating table %s.%s if it doesn't exist", self.db, self.table)
        self.query_runner(
            f"create table: {self.db}.{self.table}",
            r.expr([self.table])
            .set_difference(r.db(self.db).table_list())
            .for_each(
                r.db(self.db).table_create(
                    r.row, **self.source_options.get("create_args", {})
                )
            ),
        )
        self.query_runner(
            f"wait for {self.db}.{self.table}",
            r.db(self.db).table(self.table).wait(timeout=30),
        )
        primary_key = self.query_runner(
            f"primary key {self.db}.{self.table}",
            r.db(self.db).table(self.table).info()["primary_key"],
        )
        if self.primary_key is None:
            self.primary_key = primary_key
        elif primary_key != self.primary_key:
            raise RuntimeError(
                f"Primary key mismatch for {self.db}.{self.table}: "
                f"expected {self.primary_key}, got {primary_key}"
            )
        # Create secondary indexes

        for index_name, index_info in self.indexes.items():
            try:
                logger.debug(
                    "Creating index '%s' on %s.%s", index_name, self.db, self.table
                )
                index_function = index_info["function"]
                # If the function is a binary blob, decode and wrap with r.binary
                if (
                    isinstance(index_function, dict)
                    and index_function.get("$reql_type$") == "BINARY"
                ):
                    index_function = r.binary(base64.b64decode(index_function["data"]))
                self.query_runner(
                    f"create index {index_name} on {self.db}.{self.table}",
                    r.db(self.db)
                    .table(self.table)
                    .index_create(
                        index_name,
                        index_function,
                        multi=index_info.get("multi", False),
                        geo=index_info.get("geo", False),
                    ),
                )
            except Exception as e:
                if "already exists" not in str(e):
                    logger.error(
                        f"Failed to create index '{index_name}' on {self.db}.{self.table}: {e}"
                    )
                    raise
                else:
                    logger.debug(
                        f"Index '{index_name}' already exists on {self.db}.{self.table}"
                    )


class JsonSourceFile(SourceFile):
    """Source file for JSON data."""

    format = "json"

    def get_line(self):
        """Returns a single line from the file"""
        line = self._source.readline()
        if line:
            self.bytes_read = self._source.tell()
            self.rows_read += 1
        return line


class CsvSourceFile(SourceFile):
    """Source file for CSV data."""

    format = "csv"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # CSV-specific options
        self.no_header_row = self.source_options.get("no_header_row", False)
        self.custom_header = self.source_options.get("custom_header", None)
        self.delimiter = self.source_options.get("delimiter", ",")

    def get_line(self):
        """Returns a single line from the file"""
        line = self._source.readline()
        if line:
            self.bytes_read = self._source.tell()
            self.rows_read += 1
        return line


def parse_sources(options, files_ignored=None):
    """Parse source files from the given options."""
    logger = get_logger()

    def parse_info_file(path):
        logger.debug("Parsing info file: %s", path)
        primary_key = None
        indexes = {}
        write_hook = None

        try:
            with open(path, "r", encoding="utf-8") as info_file:
                metadata = json.load(info_file)
                if "primary_key" in metadata:
                    primary_key = metadata["primary_key"]
                if "indexes" in metadata and not options.get(
                    "no_secondary_indexes", False
                ):
                    indexes = metadata["indexes"]
                if "write_hook" in metadata:
                    write_hook = metadata["write_hook"]
                logger.debug(
                    f"Parsed info: primary_key={primary_key}, indexes={list(indexes.keys())}"
                )
        except Exception as e:
            logger.warning("Failed to parse info file %s: %s", path, e)

        return primary_key, indexes, write_hook

    sources = set()
    if files_ignored is None:
        files_ignored = []

    if options.get("directory") and options.get("file"):
        raise RuntimeError(
            "Error: Both --directory and --file cannot be specified together"
        )
    elif options.get("file"):
        logger.info("Importing single file: %s", options["file"])
        db, table = options["import_table"].split(".", 1)
        path, ext = os.path.splitext(options["file"])

        table_type_options = None
        if ext == ".json":
            table_type = JsonSourceFile
        elif ext == ".csv":
            table_type = CsvSourceFile
            table_type_options = {
                "no_header_row": options.get("no_header", False),
                "custom_header": options.get("custom_header", None),
                "delimiter": options.get("delimiter", ","),
            }
        else:
            raise Exception(f"The table type is not recognised: {ext}")

        # Parse the info file if it exists
        primary_key = options.get("pkey")
        indexes = {}
        write_hook = None

        info_path = path + ".info"
        if (
            primary_key is None or not options.get("no_secondary_indexes", False)
        ) and os.path.isfile(info_path):
            info_primary_key, info_indexes, info_write_hook = parse_info_file(info_path)
            if primary_key is None:
                primary_key = info_primary_key
            if not options.get("no_secondary_indexes", False):
                indexes = info_indexes
            if write_hook is None:
                write_hook = info_write_hook

        sources.add(
            table_type(
                source=options["file"],
                db=db,
                table=table,
                query_runner=options["query_runner"],
                primary_key=primary_key,
                indexes=indexes,
                write_hook=write_hook,
                source_options=table_type_options,
            )
        )
    elif options.get("directory"):
        logger.info("Importing from directory: %s", options["directory"])
        # Scan for all files, make sure no duplicated tables with different formats
        files_ignored = []
        dbs = False

        for root, dirs, files in os.walk(options["directory"]):
            if not dbs:
                files_ignored.extend([os.path.join(root, f) for f in files])
                # The first iteration through should be the top-level directory,
                # which contains the db folders or flat files
                dbs = True

            # If we're at the top-level directory, handle flat files (db.table.json/info)
            if root == options["directory"]:
                for filename in files:
                    path = os.path.join(root, filename)
                    table, ext = os.path.splitext(filename)
                    # If the filename is in the form db.table.json/info
                    if "." in table and ext in [".json", ".csv", ".info"]:
                        db, table = table.split(".", 1)
                        # Only process .json/.csv files (info handled below)
                        if ext == ".info":
                            continue
                        info_path = os.path.join(root, f"{db}.{table}.info")
                        if not os.path.isfile(info_path):
                            files_ignored.append(path)
                            continue
                        primary_key, indexes, write_hook = parse_info_file(info_path)
                        table_type = None
                        if ext == ".json":
                            table_type = JsonSourceFile
                        elif ext == ".csv":
                            table_type = CsvSourceFile
                        else:
                            continue
                        source = table_type(
                            source=path,
                            query_runner=options["query_runner"],
                            db=db,
                            table=table,
                            primary_key=primary_key,
                            indexes=indexes,
                            write_hook=write_hook,
                        )
                        if any(
                            s.db == source.db and s.table == source.table
                            for s in sources
                        ):
                            raise RuntimeError(
                                f"Error: Duplicate db.table found in directory tree: {source.db}.{source.table}"
                            )
                        sources.add(source)
                continue
            # Don't recurse into folders not matching our filter
            db_tables = options.get("db_tables", []) or []
            db_filter = set([db_table[0] for db_table in db_tables])
            if db_filter and os.path.basename(root) not in db_filter:
                continue

            # Collect the info
            primary_key = None
            indexes = {}
            write_hook = None

            for filename in files:
                if filename.endswith(".info"):
                    info_path = os.path.join(root, filename)
                    primary_key, indexes, write_hook = parse_info_file(info_path)
                    break

            # Process data files
            for filename in files:
                if filename.endswith((".json", ".csv")):
                    path = os.path.join(root, filename)
                    table = os.path.splitext(filename)[0]

                    # Check if this table is in our filter
                    db_tables = options.get("db_tables", []) or []
                    db_filter = set([db_table[0] for db_table in db_tables])
                    table_filter = set(
                        [db_table[1] for db_table in db_tables if len(db_table) > 1]
                    )
                    if db_filter and os.path.basename(root) not in db_filter:
                        continue
                    if table_filter and table not in table_filter:
                        continue

                    # Create source file
                    if filename.endswith(".json"):
                        source = JsonSourceFile(
                            source=path,
                            query_runner=options["query_runner"],
                            db=os.path.basename(root),
                            table=table,
                            primary_key=primary_key,
                            indexes=indexes,
                            write_hook=write_hook,
                        )
                    elif filename.endswith(".csv"):
                        source = CsvSourceFile(
                            source=path,
                            query_runner=options["query_runner"],
                            db=os.path.basename(root),
                            table=table,
                            primary_key=primary_key,
                            indexes=indexes,
                            write_hook=write_hook,
                        )

                    if any(
                        s.db == source.db and s.table == source.table for s in sources
                    ):
                        raise RuntimeError(
                            f"Error: Duplicate db.table found in directory tree: {source.db}.{source.table}"
                        )

                    sources.add(source)

    if files_ignored:
        logger.warning(
            "Unexpected files found in the specified directory. Importing a directory expects "
            "a directory from `rethinkdb export`. If you want to import individual tables "
            "import them as single files. The following files were ignored:"
        )
        for file_path in files_ignored:
            logger.warning(" %s", file_path)

    logger.info("Found %d source files to import", len(sources))
    return sources


def import_tables(options, sources):
    """Import tables from the given sources."""
    logger = get_logger()
    start_time = time.time()
    errors = []
    warnings = []

    logger.debug("Starting import with options: %s", options)
    logger.debug("Importing sources: %s", [s.db + "." + s.table for s in sources])

    # Set up signal handling for graceful interruption

    interrupt_event = threading.Event()

    def signal_handler(signum, frame):  # pylint: disable=unused-argument
        interrupt_event.set()

    signal.signal(signal.SIGINT, signal_handler)

    # Ensure batch_size, shards, replicas are valid integers
    batch_size = options.get("batch_size") or 1000
    shards = options.get("shards") or 1
    replicas = options.get("replicas") or 1
    logger.debug(
        "Using batch_size=%s, shards=%s, replicas=%s", batch_size, shards, replicas
    )

    try:
        # Set up tables
        logger.info("Setting up tables...")
        for source in sources:
            if interrupt_event.is_set():
                break
            try:
                source.setup_table()
            except Exception as e:
                logger.error("Table setup exception: %s", e)
                errors.append(Error(str(e), "", source.name))

        if errors:
            raise RuntimeError("Errors occurred during table setup")

        # Import data
        total_sources = len(sources)
        completed_sources = 0

        logger.info("Starting data import...")
        for source in sources:
            if interrupt_event.is_set():
                break

            try:
                logger.debug("Importing data for %s.%s", source.db, source.table)
                # Read and insert data
                docs = []
                if source.format == "json":
                    # Read JSON data
                    json_data = source._source.read()
                    if json_data.strip():
                        docs = json.loads(json_data)
                elif source.format == "csv":
                    # Read CSV data
                    reader = csv.DictReader(
                        source._source,
                        delimiter=source.delimiter,
                        fieldnames=source.custom_header,
                    )
                    if source.no_header_row:
                        next(reader)  # Skip header row
                    docs = list(reader)

                # Insert documents
                if docs:
                    logger.debug(
                        "Inserting %d documents into %s.%s",
                        len(docs),
                        source.db,
                        source.table,
                    )
                    for i in range(0, len(docs), batch_size):
                        batch = docs[i : i + batch_size]
                        source.query_runner(
                            f"insert batch into {source.db}.{source.table}",
                            r.db(source.db).table(source.table).insert(batch),
                        )
                        source.add_rows_written(len(batch))

                completed_sources += 1

                # Update progress
                if not options.get("quiet", False):
                    print_progress(completed_sources, total_sources, "Importing tables")

            except Exception as e:
                logger.error(
                    "Data import exception for %s.%s: %s", source.db, source.table, e
                )
                if logger.isEnabledFor(logging.DEBUG):
                    traceback.print_exc()
                errors.append(Error(str(e), "", source.name))

        # Final progress update
        if not options.get("quiet", False):
            print_progress(completed_sources, total_sources, "Importing tables")

        # Report statistics
        if not options.get("quiet", False):

            def plural(num, text):
                return f"{num} {text}{'' if num == 1 else 's'}"

            total_rows = sum(source.rows_written for source in sources)
            logger.info(
                " %s imported to %s in %.2f secs",
                plural(total_rows, "row"),
                plural(len(sources), "table"),
                time.time() - start_time,
            )

    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    # Report errors and warnings
    for error in errors:
        logger.error("%s", error.message)

    for warning in warnings:
        logger.warning("%s", warning[1])

    if interrupt_event.is_set():
        raise RuntimeError("Interrupted")
    if errors:
        raise RuntimeError("Errors occurred during import")
    if warnings:
        raise RuntimeError("Warnings occurred during import")


@click.command()
@common_options
@click.option(
    "-f",
    "--file",
    "input_file",
    default=None,
    metavar="FILE",
    help="File to import data from (e.g., my_export.tar.gz).",
)
@click.option(
    "-d",
    "--directory",
    "input_dir",
    default=None,
    metavar="DIRECTORY",
    help="Directory to import data from.",
)
@click.option(
    "--table",
    "import_table",
    default=None,
    metavar="DB.TABLE",
    help="Table to import the data into (for single file import).",
)
@click.option(
    "-i",
    "--import",
    "import_",
    multiple=True,
    metavar="DB|DB.TABLE",
    help="Restore only the given database or table.",
)
@click.option(
    "--fields",
    default=None,
    metavar="FIELD,...",
    help="Limit which fields to use when importing.",
)
@click.option(
    "--format",
    default="json",
    metavar="json|csv|jsongz",
    type=click.Choice(["json", "csv", "jsongz"], case_sensitive=False),
    help="Format of the file.",
)
@click.option(
    "--pkey",
    default=None,
    metavar="PRIMARY_KEY",
    help="Field to use as the primary key.",
)
@click.option(
    "--clients",
    default=8,
    metavar="CLIENTS",
    show_default=True,
    type=int,
    help="Client connections to use.",
)
@click.option(
    "--hard-durability",
    is_flag=True,
    default=False,
    help="Use hard durability writes.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Import even if a table already exists.",
)
@click.option(
    "--batch-size",
    default=None,
    metavar="SIZE",
    type=int,
    help="Batch size for inserts.",
)
@click.option(
    "--shards",
    default=None,
    metavar="SHARDS",
    type=int,
    help="Shards to setup on created tables.",
)
@click.option(
    "--replicas",
    default=None,
    metavar="REPLICAS",
    type=int,
    help="Replicas to setup on created tables.",
)
@click.option(
    "--no-secondary-indexes",
    is_flag=True,
    default=False,
    help="Do not create secondary indexes.",
)
@click.option(
    "--delimiter",
    default=",",
    metavar="CHARACTER",
    help="CSV field delimiter.",
)
@click.option(
    "--no-header",
    is_flag=True,
    default=False,
    help="CSV file has no header row.",
)
@click.option(
    "--custom-header",
    default=None,
    metavar="FIELD,...",
    help="Header to use for CSV (overrides file header).",
)
def cmd_import(
    input_file,
    input_dir,
    import_table,
    import_,
    fields,
    import_format,
    pkey,
    clients,
    hard_durability,
    force,
    batch_size,
    shards,
    replicas,
    no_secondary_indexes,
    delimiter,
    no_header,
    custom_header,
    **connection_args,
):
    """Import data into RethinkDB from files or directories."""
    # Setup logging for this command

    debug = connection_args.get("debug", False)
    quiet = connection_args.get("quiet", False)

    if debug:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        log_level = logging.DEBUG
    else:
        log_format = "%(message)s"
        log_level = logging.INFO if not quiet else logging.WARNING

    logging.basicConfig(
        level=log_level,
        format=log_format,
        stream=sys.stdout,
        force=True,
    )

    logger = get_logger()
    logger.debug("Import command started with debug logging enabled")
    start_time = time.time()

    try:
        # Validate input
        if not input_file and not input_dir:
            raise click.UsageError("Either --file or --directory must be specified.")

        if input_file and input_dir:
            raise click.UsageError("Cannot specify both --file and --directory.")

        if input_file and not import_table:
            raise click.UsageError("--table is required when importing from a file.")

        if custom_header and no_header:
            pass  # This is fine
        elif not custom_header and no_header:
            raise click.UsageError("--no-header requires --custom-header.")

        # Prepare temporary directory if importing from archive
        temp_dir_used = False
        work_dir = input_dir

        if input_file:
            if not input_file.endswith(".tar.gz"):
                raise click.UsageError("Input file must be a .tar.gz archive.")

            logger.info("Extracting archive: %s", input_file)
            work_dir = tempfile.mkdtemp(prefix="rethinkdb_import_")
            temp_dir_used = True

            with tarfile.open(input_file, "r:gz") as tar:
                # Find the top-level directory in the archive
                top_level_dirs = {os.path.commonpath(tar.getnames())}
                if len(top_level_dirs) == 1:
                    top_level_dir = list(top_level_dirs)[0]
                    # Extract only members within that directory
                    members_to_extract = [
                        m for m in tar.getmembers() if m.name.startswith(top_level_dir)
                    ]
                    tar.extractall(path=work_dir, members=members_to_extract)
                    # Adjust work_dir to be the extracted top-level directory
                    work_dir = os.path.join(work_dir, top_level_dir)
                else:
                    tar.extractall(path=work_dir)

        # Create a simple query runner function
        def query_runner(description, query):
            """Simple query runner that executes queries and handles errors."""
            try:
                return query.run(conn)
            except Exception as e:
                raise RuntimeError(f"Error in {description}: {e}")

        # Parse sources
        options = {
            "import_table": import_table,
            "db_tables": parse_list_args(import_) if import_ else None,
            "pkey": pkey,
            "fields": fields.split(",") if fields else None,
            "format": import_format,
            "delimiter": delimiter,
            "no_header": no_header,
            "custom_header": custom_header.split(",") if custom_header else None,
            "batch_size": batch_size,
            "shards": shards,
            "replicas": replicas,
            "no_secondary_indexes": no_secondary_indexes,
            "hard_durability": hard_durability,
            "force": force,
            "clients": clients,
            "quiet": connection_args.get("quiet", False),
            "debug": connection_args.get("debug", False),
            "query_runner": query_runner,
        }

        # Only set the relevant source option
        if input_file:
            # For tar.gz archives, we extract to a directory and treat as directory import
            if input_file.endswith(".tar.gz"):
                options["directory"] = work_dir
            else:
                # For single files, we need the table specification
                options["file"] = input_file
        elif work_dir:
            options["directory"] = work_dir

        # Get connection
        logger.info("Connecting to RethinkDB...")
        conn = get_connection(**connection_args)

        try:
            sources = parse_sources(options)
            import_tables(options, sources)
        finally:
            conn.close()

        if temp_dir_used:
            logger.debug("Cleaning up temporary directory")
            shutil.rmtree(
                os.path.dirname(work_dir)
            )  # Clean up the parent of the extracted dir

        logger.info("Import completed in %.2f seconds", time.time() - start_time)
        return 0

    except Exception as e:
        logger.error("Import failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(cmd_import())

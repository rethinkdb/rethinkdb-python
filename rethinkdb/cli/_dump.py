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
# This file is a new implementation based on the legacy dump script
# using the modern click-based CLI framework and ThreadPoolExecutor.

# pylint: disable=too-many-arguments,too-many-locals,too-many-branches

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import threading
from typing import Any, Dict, List, Optional

import click

import rethinkdb as r
from rethinkdb import errors
from rethinkdb.cli.utils import (
    common_options,
    get_connection,
    get_logger,
    json_default,
    parse_list_args,
    print_progress,
)


def default_output_file():
    """Generate a default archive name based on the current time."""
    now = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    return f"rethinkdb_dump_{now}.tar.gz"


def get_all_tables(conn, dbs: Optional[List[str]] = None) -> list[tuple[str, str]]:
    """Return a list of (db, table) tuples for all tables in the given databases."""
    tables: list[tuple[str, str]] = []
    db_list = dbs
    if db_list is None:
        db_list = r.db_list().run(conn)

    for db in db_list:
        if db == "rethinkdb":
            continue
        for table in r.db(db).table_list().run(conn):  # type: ignore[call-arg]
            tables.append((db, table))
    return tables


def dump_worker(
    db: str,
    table: str,
    work_dir: str,
    read_outdated: bool,
    connection_args: Dict[str, Any],
    progress_state: Dict[str, Any],
):
    """
    Dump a single table to JSON and metadata files.
    This function is designed to be run in a worker thread.
    """
    logger = get_logger()
    conn = None
    try:
        logger.debug("Starting dump of %s.%s", db, table)
        conn = get_connection(**connection_args)

        # - Create database directory
        db_dir = os.path.join(work_dir, db)
        os.makedirs(db_dir, exist_ok=True)

        # - Write metadata file
        logger.debug("Writing metadata for %s.%s", db, table)
        table_info = r.db(db).table(table).info().run(conn)  # type: ignore[call-arg]
        primary_key = table_info["primary_key"]

        metadata = {
            "primary_key": primary_key,
            "indexes": {},
            "write_hook": None,
        }
        for index in r.db(db).table(table).index_status().run(conn):  # type: ignore[call-arg]
            metadata["indexes"][index["index"]] = {
                "geo": index.get("geo", False),
                "multi": index.get("multi", False),
            }

        # - Write metadata file
        metadata_file = os.path.join(db_dir, f"{table}.info")
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, default=json_default, indent=2)

        # - Write data file
        logger.debug("Writing data for %s.%s", db, table)
        data_file = os.path.join(db_dir, f"{table}.json")
        with open(data_file, "w", encoding="utf-8") as f:
            query = r.db(db).table(table)
            if read_outdated:
                query = query.read_outdated()
            cursor = query.run(conn)
            for row in cursor:
                f.write(json.dumps(row, default=json_default) + "\n")

        # - Update progress
        with progress_state["lock"]:
            progress_state["completed"] += 1
            if not progress_state.get("quiet", False):
                print_progress(
                    progress_state["completed"],
                    progress_state["total"],
                    f"Dumped {db}.{table}",
                )

        logger.debug("Completed dump of %s.%s", db, table)

    except Exception as e:
        logger.error("Error dumping %s.%s: %s", db, table, e)
        raise
    finally:
        if conn:
            conn.close()


@click.command()
@common_options
@click.option(
    "-f",
    "--file",
    "output_file",
    default=None,
    metavar="FILE",
    help="File to write archive to (defaults to rethinkdb_dump_DATE_TIME.tar.gz); "
    "if FILE is -, use standard output.",
)
@click.option(
    "-e",
    "--export",
    multiple=True,
    metavar="DB|DB.TABLE",
    help="Limit dump to the given database or table (may be specified multiple times).",
)
@click.option(
    "--temp-dir",
    default=None,
    metavar="DIRECTORY",
    help="The directory to use for intermediary results.",
)
@click.option(
    "--overwrite-file",
    is_flag=True,
    default=False,
    help="Overwrite --file if it exists.",
)
@click.option(
    "--clients",
    default=3,
    metavar="NUM",
    show_default=True,
    type=int,
    help="Number of tables to export simultaneously.",
)
@click.option(
    "--read-outdated",
    is_flag=True,
    default=False,
    help="Use outdated read mode.",
)
def cmd_dump(
    output_file,
    export,
    temp_dir,
    overwrite_file,
    clients,
    read_outdated,
    **connection_args,
):
    """Creates an archive of data from a RethinkDB cluster."""
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
    logger.debug("Dump command started with debug logging enabled")

    if not connection_args.get("quiet"):
        logger.info(
            "NOTE: 'rethinkdb-dump' saves data, secondary indexes, and write hooks, "
            "but does *not* save cluster metadata. You will need to recreate your "
            "cluster setup yourself after you run 'rethinkdb-restore'."
        )

    # -- Validate options
    out_file = output_file or default_output_file()
    if out_file != "-" and os.path.exists(out_file) and not overwrite_file:
        raise click.UsageError(
            f"Output file already exists: {out_file}. Use --overwrite-file to overwrite."
        )

    # -- Establish connection
    logger.info("Connecting to RethinkDB...")
    conn = None
    try:
        conn = get_connection(**connection_args)
    except errors.ReqlDriverError as e:
        logger.error("Error connecting to RethinkDB: %s", e)
        return 1

    # -- Determine what to dump
    logger.info("Determining tables to dump...")
    tables_to_dump = []
    if not export:
        tables_to_dump = get_all_tables(conn)
    else:
        parsed_exports = parse_list_args(export)
        all_dbs = r.db_list().run(conn)

        for db, tables in parsed_exports.items():
            if db not in all_dbs:
                raise click.UsageError(f"Database '{db}' not found.")
            if not tables:
                tables_to_dump.extend(get_all_tables(conn, [db]))
            else:
                all_tables_in_db = r.db(db).table_list().run(conn)  # type: ignore[call-arg]
                for table in tables:
                    if table not in all_tables_in_db:
                        raise click.UsageError(f"Table '{db}.{table}' not found.")
                    tables_to_dump.append((db, table))

    if not tables_to_dump:
        logger.warning("No tables to dump.")
        return 0

    logger.info("Found %d tables to dump", len(tables_to_dump))

    # -- Prepare temporary directory
    temp_dir_created = False
    if temp_dir:
        work_dir = temp_dir
        os.makedirs(work_dir, exist_ok=True)
        logger.debug("Using provided temp directory: %s", temp_dir)
    else:
        work_dir = tempfile.mkdtemp(prefix="rethinkdb_dump_")
        temp_dir_created = True
        logger.debug("Created temporary directory: %s", work_dir)

    # -- Get total row count for progress bar
    logger.info("Counting total rows for progress tracking...")
    total_rows = 0
    for db, table in tables_to_dump:
        count = r.db(db).table(table).count().run(conn)  # type: ignore[call-arg]
        total_rows += count

    conn.close()  # Close main connection, workers will use their own

    # -- Start dump
    logger.info(
        "Starting dump of %d tables with %d workers...",
        len(tables_to_dump),
        clients,
    )

    # -- Create progress tracking state
    progress_state = {
        "completed": 0,
        "total": len(tables_to_dump),
        "lock": threading.Lock(),
        "quiet": connection_args.get("quiet", False),
    }

    # -- Run dump workers
    with ThreadPoolExecutor(max_workers=clients) as executor:
        futures = []
        for db, table in tables_to_dump:
            future = executor.submit(
                dump_worker,
                db,
                table,
                work_dir,
                read_outdated,
                connection_args,
                progress_state,
            )
            futures.append(future)

        # -- Wait for completion and handle errors
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error("Worker failed: %s", e)
                if temp_dir_created:
                    shutil.rmtree(work_dir, ignore_errors=True)
                return 1

    # -- Create archive
    logger.info("Creating archive...")
    try:
        if out_file == "-":
            # Write to stdout
            with tarfile.open(fileobj=sys.stdout.buffer, mode="w:gz") as tar:
                tar.add(work_dir, arcname="")
        else:
            # Write to file
            with tarfile.open(out_file, "w:gz") as tar:
                tar.add(work_dir, arcname="")
        logger.info("Archive created successfully: %s", out_file)
    except Exception as e:
        logger.error("Error creating archive: %s", e)
        if temp_dir_created:
            shutil.rmtree(work_dir, ignore_errors=True)
        return 1

    # -- Cleanup
    if temp_dir_created:
        shutil.rmtree(work_dir, ignore_errors=True)
        logger.debug("Cleaned up temporary directory")

    logger.info("Dump completed successfully")
    return 0


def main():
    """Main entry point for the dump command."""
    sys.exit(cmd_dump())


if __name__ == "__main__":
    main()

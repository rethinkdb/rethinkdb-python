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
`rethinkdb restore` loads data into a RethinkDB cluster from an archive
"""

import copy
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import time
import traceback
from typing import Any, Dict, List

import click

from rethinkdb.cli._import import import_tables, parse_sources
from rethinkdb.cli.utils import (
    common_options,
    get_connection,
    get_logger,
    parse_list_args,
)


def do_unzip(temp_dir: str, options: Dict[str, Any]) -> List[str]:
    """Extract the tarfile to the filesystem."""
    logger = get_logger()
    tables_to_export = set(options.get("db_tables", []))
    top_level = None
    files_ignored = []
    files_found = False
    archive = None

    in_file = options["in_file"]
    logger.info("Extracting archive: %s", in_file)

    tarfile_options = {
        "mode": "r:*",
        "fileobj" if hasattr(in_file, "read") else "name": in_file,
    }

    try:
        archive = tarfile.open(**tarfile_options)

        for tarinfo in archive:
            # Skip anything but files
            if not tarinfo.isfile():
                continue

            # Normalize the path
            relpath = os.path.relpath(
                os.path.realpath(tarinfo.name.strip().lstrip(os.sep))
            )

            # Skip things that try to jump out of the folder
            if relpath.startswith(os.path.pardir):
                files_ignored.append(tarinfo.name)
                continue

            # Skip file types other than what we use
            if not os.path.splitext(relpath)[1] in (".json", ".csv", ".info"):
                files_ignored.append(tarinfo.name)
                continue

            # Ensure this looks like our structure
            try:
                top, db, file_name = relpath.split(os.sep)
            except ValueError:
                raise RuntimeError(
                    "Error: Archive file has an unexpected directory structure: %s"
                    % tarinfo.name
                )

            if not top_level:
                top_level = top
            elif top != top_level:
                raise RuntimeError(
                    "Error: Archive file has an unexpected directory structure (%s vs %s)"
                    % (top, top_level)
                )

            # Filter out tables we are not looking for
            table = os.path.splitext(file_name)[0]
            if tables_to_export and not (
                (db, table) in tables_to_export or (db, None) in tables_to_export
            ):
                continue

            # Write the file out
            files_found = True
            dest_path = os.path.join(temp_dir, db, file_name)

            if not os.path.exists(os.path.dirname(dest_path)):
                os.makedirs(os.path.dirname(dest_path))

            logger.debug("Extracting %s to %s", tarinfo.name, dest_path)
            with open(dest_path, "wb") as dest:
                source = archive.extractfile(tarinfo)
                if source is not None:
                    chunk = source.read(1024 * 128)
                    while chunk:
                        dest.write(chunk)
                        chunk = source.read(1024 * 128)
                    source.close()
                else:
                    raise RuntimeError(f"Failed to extract file {tarinfo.name}")

            if not os.path.isfile(dest_path):
                raise AssertionError(f"Was not able to write {dest_path}")

    finally:
        if archive:
            archive.close()

    if not files_found:
        raise RuntimeError("Error: Archive file had no files")

    if files_ignored:
        logger.debug("Ignored %d files during extraction", len(files_ignored))

    logger.info("Archive extraction completed")
    # Send the location and ignored list back to our caller
    return files_ignored

    # Send the location and ignored list back to our caller
    return files_ignored


def do_restore(options: Dict[str, Any]) -> None:
    """Main restore function."""
    logger = get_logger()

    # Create a temporary directory to store the extracted data
    temp_dir = tempfile.mkdtemp(dir=options.get("temp_dir"))
    logger.debug("Created temporary directory: %s", temp_dir)

    try:
        # Extract the archive
        start_time = time.time()
        do_unzip(temp_dir, options)
        logger.info(
            "Archive extraction completed in %.2f seconds", time.time() - start_time
        )

        # Default _import options
        import_options = copy.copy(options)
        import_options["fields"] = None
        import_options["directory"] = temp_dir
        import_options["file"] = None

        # The import system expects files in the format db.table.json and db.table.info
        # But the dump format creates db/table.json structure (after extraction)
        # We need to reorganize the files to match the import system's expectations
        logger.debug("Checking archive structure and reorganizing files if needed...")
        db_dirs = [
            d for d in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, d))
        ]
        if db_dirs:
            # Check if these are database directories (contain .json/.info files)
            has_db_structure = False
            for db_dir in db_dirs:
                db_path = os.path.join(temp_dir, db_dir)
                files = os.listdir(db_path)
                if any(f.endswith(".json") or f.endswith(".info") for f in files):
                    has_db_structure = True
                    break

            if has_db_structure:
                logger.debug("Reorganizing files from dump format to import format...")
                # This is the dump format, reorganize files
                for db_dir in db_dirs:
                    db_path = os.path.join(temp_dir, db_dir)
                    for filename in os.listdir(db_path):
                        if filename.endswith((".json", ".info")):
                            # Move file from db/table.json to temp_dir/db.table.json
                            src_path = os.path.join(db_path, filename)
                            table_name = os.path.splitext(filename)[0]
                            dst_filename = (
                                f"{db_dir}.{table_name}{os.path.splitext(filename)[1]}"
                            )
                            dst_path = os.path.join(temp_dir, dst_filename)
                            shutil.move(src_path, dst_path)
                            logger.debug("Moved %s to %s", src_path, dst_path)

                # Remove the database directories
                for db_dir in db_dirs:
                    db_path = os.path.join(temp_dir, db_dir)
                    if os.path.isdir(db_path):
                        shutil.rmtree(db_path)

        # Create a simple query runner function
        def query_runner(description, query):
            """Simple query runner that executes queries and handles errors."""
            try:
                return query.run(conn)
            except Exception as e:
                raise RuntimeError("Error in %s: %s" % (description, e))

        import_options["query_runner"] = query_runner

        # Get connection
        logger.info("Connecting to RethinkDB...")
        conn = get_connection(
            **{
                k: v
                for k, v in options.items()
                if k
                in [
                    "connect",
                    "driver_port",
                    "host_name",
                    "user",
                    "password",
                    "password_file",
                    "tls_cert",
                    "quiet",
                    "debug",
                ]
            }
        )

        try:
            sources = parse_sources(import_options)

            # Run the import
            logger.info("Starting data import...")
            try:
                import_tables(import_options, sources)
            except RuntimeError as exc:
                if options.get("debug", False):
                    traceback.print_exc()

                if str(exc) == "Warnings occurred during import":
                    raise RuntimeError(
                        "Warning: import did not create some secondary indexes."
                    )
                else:
                    error_string = str(exc)
                    if error_string.startswith("Error: "):
                        error_string = error_string[len("Error: ") :]
                    raise RuntimeError(f"Error: import failed: {error_string}")

            logger.info("Restore completed successfully")

        finally:
            conn.close()

    finally:
        # Clean up temporary directory
        logger.debug("Cleaning up temporary directory")
        shutil.rmtree(temp_dir)


@click.command()
@common_options
@click.argument("archive", required=True)
@click.option(
    "-i",
    "--import",
    "import_",
    multiple=True,
    metavar="DB|DB.TABLE",
    help="Limit restore to the given database or table (may be specified multiple times)",
)
@click.option(
    "--temp-dir",
    metavar="DIR",
    default=None,
    help="Directory to use for intermediary results",
)
@click.option(
    "--clients",
    metavar="CLIENTS",
    default=8,
    show_default=True,
    help="Client connections to use (default: 8)",
    type=int,
)
@click.option(
    "--hard-durability",
    is_flag=True,
    default=False,
    help="Use hard durability writes (slower, uses less memory)",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Import data even if a table already exists",
)
@click.option(
    "--no-secondary-indexes",
    is_flag=True,
    default=False,
    help="Do not create secondary indexes for the restored tables",
)
@click.option(
    "--writers-per-table",
    default=None,
    type=int,
    hidden=True,
)
@click.option(
    "--batch-size",
    default=None,
    type=int,
    hidden=True,
)
@click.option(
    "--shards",
    default=None,
    metavar="SHARDS",
    help="Shards to setup on created tables (default: 1)",
    type=int,
)
@click.option(
    "--replicas",
    default=None,
    metavar="REPLICAS",
    help="Replicas to setup on created tables (default: 1)",
    type=int,
)
@click.option(
    "--pkey",
    default=None,
    help="Primary key to use for restored tables (default: id).",
)
def cmd_restore(
    archive,
    import_,
    temp_dir,
    clients,
    hard_durability,
    force,
    no_secondary_indexes,
    writers_per_table,
    batch_size,
    shards,
    replicas,
    pkey,
    **connection_args,
):
    """Restore loads data into a RethinkDB cluster from an archive."""
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
    logger.debug("Restore command started with debug logging enabled")

    # Validate archive argument
    if archive == "-":
        in_file = sys.stdin
        logger.info("Reading archive from stdin")
    else:
        if not os.path.isfile(archive):
            raise click.UsageError("Archive file does not exist: %s" % archive)
        in_file = os.path.realpath(archive)
        logger.info("Restoring from archive: %s", archive)

    # Validate temp_dir if specified
    if temp_dir:
        if not os.path.isdir(temp_dir):
            raise click.UsageError(
                "Temporary directory doesn't exist or is not a directory: %s" % temp_dir
            )
        if not os.access(temp_dir, os.W_OK):
            raise click.UsageError("Temporary directory inaccessible: %s" % temp_dir)
        logger.debug("Using temporary directory: %s", temp_dir)

    # Prepare options for restore
    options = {
        "in_file": in_file,
        "temp_dir": temp_dir,
        "clients": clients,
        "hard_durability": hard_durability,
        "force": force,
        "no_secondary_indexes": no_secondary_indexes,
        "writers_per_table": writers_per_table,
        "batch_size": batch_size,
        "shards": shards,
        "replicas": replicas,
        "pkey": pkey,
        "db_tables": parse_list_args(import_) if import_ else [],
        "create_args": {},
    }

    # Add connection arguments
    options.update(connection_args)

    # Set up create_args for table creation
    if shards:
        options["create_args"]["shards"] = shards
    if replicas:
        options["create_args"]["replicas"] = replicas

    try:
        do_restore(options)
        return 0
    except RuntimeError as exc:
        logger.error("Restore failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(cmd_restore())

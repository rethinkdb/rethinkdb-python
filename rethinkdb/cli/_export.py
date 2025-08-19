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
# This file is a new implementation based on the legacy export script
# using the modern click-based CLI framework and ThreadPoolExecutor.

# pylint: disable=too-many-arguments,too-many-locals,too-many-branches

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime
import gzip
import json
import logging
import os
import sys
import threading
from typing import Any, Dict, List, Optional

import click

from rethinkdb import errors, r
from rethinkdb.cli.utils import (
    common_options,
    get_connection,
    get_logger,
    json_default,
    parse_list_args,
    print_progress,
)


def default_output_dir():
    """Generate a default directory name based on the current time."""
    now = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    return f"rethinkdb_export_{now}"


def get_all_tables(conn, dbs: Optional[List[str]] = None) -> list[tuple[str, str]]:
    """Return a list of (db, table) tuples for all tables in the given databases."""
    tables: list[tuple[str, str]] = []
    db_list = dbs
    if db_list is None:
        db_list = r.db_list().run(conn)

    for db in db_list:
        if db == "rethinkdb":
            continue
        for table in r.db(db).table_list().run(conn):
            tables.append((db, table))
    return tables


def export_worker(
    db: str,
    table: str,
    out_dir: str,
    fields: Optional[List[str]],
    export_format: str,
    compression_level: int,
    delimiter: str,
    read_outdated: bool,
    connection_args: Dict[str, Any],
    progress_state: Dict[str, Any],
):
    """
    Export a single table from the database.
    This function is designed to be run in a worker thread.
    """
    conn = None
    try:
        conn = get_connection(**connection_args)

        # - Write metadata file
        table_info = r.db(db).table(table).info().run(conn)
        primary_key = table_info["primary_key"]

        metadata = {
            "primary_key": primary_key,
            "indexes": {},
            "write_hook": None,
        }
        for index in r.db(db).table(table).index_status().run(conn):
            metadata["indexes"][index["index"]] = {
                "function": index["function"],
                "geo": index["geo"],
                "multi": index["multi"],
                "outdated": index["outdated"],
            }

        write_hook = r.db(db).table(table).get_write_hook().run(conn)
        if write_hook:
            metadata["write_hook"] = write_hook

        info_file = os.path.join(out_dir, f"{db}.{table}.info")
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=True, default=json_default)

        # - Prepare output file and writer
        is_json = export_format in ("json", "ndjson", "jsongz")
        ext = {
            "json": "json",
            "ndjson": "ndjson",
            "jsongz": "jsongz",
            "csv": "csv",
        }[export_format]

        out_file = os.path.join(out_dir, f"{db}.{table}.{ext}")
        gzipped = export_format == "jsongz"

        if gzipped:
            file_handle = gzip.open(
                out_file, "wt", encoding="utf-8", compresslevel=compression_level
            )
        else:
            file_handle = open(out_file, "w", encoding="utf-8", newline="")

        with file_handle as f:
            if is_json and export_format != "ndjson":
                f.write("[\n")

            csv_writer = None
            if not is_json:
                csv_writer = csv.writer(f, delimiter=delimiter)
                if fields:
                    csv_writer.writerow(fields)

            run_options = {}
            if read_outdated:
                run_options["read_mode"] = "outdated"

            last_pk = None
            rows_written = 0
            while True:
                try:
                    query = r.db(db).table(table).order_by(index=primary_key)
                    if last_pk is not None:
                        query = query.between(last_pk, r.maxval, left_bound="open")

                    cursor = query.run(conn, **run_options)

                    for doc in cursor:
                        row_to_write = doc
                        if fields:
                            row_to_write = {key: doc.get(key) for key in fields}

                        if is_json:
                            if rows_written > 0 and export_format != "ndjson":
                                f.write(",\n")
                            f.write(json.dumps(row_to_write, default=json_default))
                            if export_format == "ndjson":
                                f.write("\n")
                        else:  # csv
                            if fields and csv_writer is not None:
                                csv_row = []
                                for field in fields:
                                    val = row_to_write.get(field)
                                    if isinstance(val, (dict, list, bool)):
                                        csv_row.append(
                                            json.dumps(val, default=json_default)
                                        )
                                    else:
                                        csv_row.append(val)
                                csv_writer.writerow(csv_row)

                        rows_written += 1
                        last_pk = doc[primary_key]

                        with progress_state["lock"]:
                            progress_state["rows_done"] += 1

                    break  # Finished successfully

                except errors.ReqlDriverError as err:
                    click.echo(
                        f"\nWarning: Driver error exporting {db}.{table}: {err}. Retrying.",
                        err=True,
                    )
                    if conn:
                        conn.close()
                    conn = get_connection(**connection_args)

            if is_json and format != "ndjson":
                f.write("\n]\n")

        return db, table, rows_written

    finally:
        if conn:
            conn.close()


@click.command()
@common_options
@click.option(
    "-d",
    "--directory",
    "output",
    default=None,
    metavar="DIRECTORY",
    help="Directory to output to (default: rethinkdb_export_DATE_TIME)",
)
@click.option(
    "-e",
    "--export",
    multiple=True,
    metavar="DB|DB.TABLE",
    help="Limit export to the given database or table (may be specified multiple times).",
)
@click.option(
    "--fields",
    default=None,
    metavar="<FIELD>,...",
    help="Export only specified fields (required for CSV format).",
)
@click.option(
    "--format",
    default="json",
    metavar="json|csv|ndjson|jsongz",
    type=click.Choice(["json", "csv", "ndjson", "jsongz"], case_sensitive=False),
    help="Format to write (defaults to json).",
)
@click.option(
    "--compression-level",
    default=6,
    metavar="LEVEL",
    show_default=True,
    type=click.IntRange(1, 9),
    help="Compression level for jsongz format (1-9).",
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
@click.option(
    "--delimiter",
    default=",",
    metavar="CHARACTER",
    help="Character for field delimiter in CSV, or '\\t' for tab.",
)
def cmd_export(
    clients,
    compression_level,
    delimiter,
    export,
    fields,
    export_format,
    output,
    read_outdated,
    **connection_args,
):
    """Export data from a RethinkDB cluster into a directory."""
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
    logger.debug("Export command started with debug logging enabled")

    # -- Validate options
    if export_format == "csv" and not fields:
        raise click.UsageError("CSV format requires specifying the --fields option.")

    if export_format != "csv" and delimiter != ",":
        raise click.UsageError("--delimiter is only valid for CSV format.")

    if export_format != "jsongz" and compression_level != 6:
        raise click.UsageError("--compression-level is only valid for jsongz format.")

    if delimiter == "\\t":
        delimiter = "\t"
    elif len(delimiter) != 1:
        raise click.UsageError("Delimiter must be a single character.")

    field_list = fields.split(",") if fields else None

    # -- Establish connection
    conn = None
    try:
        conn = get_connection(**connection_args)
    except errors.ReqlDriverError as e:
        logger.error("Error connecting to RethinkDB: %s", e)
        return

    # -- Determine what to export
    tables_to_export = []
    if not export:
        tables_to_export = get_all_tables(conn)
    else:
        parsed_exports = parse_list_args(export)
        all_dbs = r.db_list().run(conn)

        for db, tables in parsed_exports.items():
            if db not in all_dbs:
                raise click.UsageError("Database '%s' not found." % db)
            if not tables:
                tables_to_export.extend(get_all_tables(conn, [db]))
            else:
                all_tables = r.db(db).table_list().run(conn)
                for table in tables:
                    if table not in all_tables:
                        raise click.UsageError("Table '%s.%s' not found." % (db, table))
                    tables_to_export.append((db, table))

    if not tables_to_export:
        logger.info("No tables to export.")
        return

    # -- Prepare output directory
    out_dir = output or default_output_dir()
    os.makedirs(out_dir, exist_ok=True)

    # -- Get total row count for progress bar
    total_rows = 0
    for db, table in tables_to_export:
        count = r.db(db).table(table).count().run(conn)
        total_rows += count

    conn.close()  # Close main connection, workers will use their own

    # -- Start export
    progress_state = {"rows_done": 0, "lock": threading.Lock()}

    with ThreadPoolExecutor(max_workers=clients) as executor:
        futures = {
            executor.submit(
                export_worker,
                db,
                table,
                out_dir,
                field_list,
                export_format,
                compression_level,
                delimiter,
                read_outdated,
                connection_args,
                progress_state,
            ): (db, table)
            for db, table in tables_to_export
        }

        while any(not future.done() for future in futures):
            print_progress(progress_state["rows_done"], total_rows, prefix="Export")
            threading.Event().wait(0.1)

        print_progress(progress_state["rows_done"], total_rows, prefix="Export")

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                db, table = futures[future]
                logger.error("Error exporting table %s.%s: %s", db, table, e)

    logger.info("Export complete. Data written to %s", out_dir)

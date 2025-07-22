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
# This file is a new implementation based on the legacy index rebuild script
# using the modern click-based CLI framework and ThreadPoolExecutor.

# pylint: disable=too-many-arguments,too-many-locals

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Any, Dict, List, Optional

import click

from rethinkdb import errors, r
from rethinkdb.cli.utils import (
    common_options,
    get_connection,
    parse_list_args,
    print_progress,
)


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


def rebuild_worker(
    db: str,
    table: str,
    index: str,
    connection_args: Dict[str, Any],
    progress_state: Dict[str, Any],
):
    """
    Rebuild a single index.
    This function is designed to be run in a worker thread.
    """
    conn = None
    try:
        conn = get_connection(**connection_args)

        tmp_index_prefix = "$reql_temp_index$_"
        temp_name = f"{tmp_index_prefix}{index}"

        # Drop leftover temp index if present
        existing = r.db(db).table(table).index_list().run(conn)
        if temp_name in existing:
            r.db(db).table(table).index_drop(temp_name).run(conn)

        # Fetch original index properties
        status = r.db(db).table(table).index_status(index).nth(0).run(conn)
        func = status.get("function")
        geo = bool(status.get("geo", False))
        multi = bool(status.get("multi", False))

        # Create temp index with same definition
        if func:
            r.db(db).table(table).index_create(
                temp_name, func, geo=geo, multi=multi
            ).run(conn)
        else:
            r.db(db).table(table).index_create(temp_name, geo=geo, multi=multi).run(
                conn
            )

        # Wait for temp index build
        r.db(db).table(table).index_wait(temp_name).run(conn)

        # Rename temp index over original
        r.db(db).table(table).index_rename(temp_name, index, overwrite=True).run(conn)

        with progress_state["lock"]:
            progress_state["indexes_done"] += 1

        return db, table, index
    finally:
        if conn:
            conn.close()


@click.command()
@common_options
@click.option(
    "-r",
    "--rebuild",
    "db_table",
    multiple=True,
    metavar="DB|DB.TABLE",
    help="Databases or tables to rebuild indexes on (default: all).",
)
@click.option(
    "-n",
    "concurrent",
    default=1,
    metavar="NUM",
    show_default=True,
    type=int,
    help="Rebuild NUM indexes concurrently.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Rebuild all indexes, not just outdated ones.",
)
def cmd_index_rebuild(db_table, concurrent, force, **connection_args):
    """Recreates outdated secondary indexes in a cluster."""

    # -- Establish connection
    conn = None
    try:
        conn = get_connection(**connection_args)
    except errors.ReqlDriverError as e:
        click.echo("Error connecting to RethinkDB: %s" % e, err=True)
        return

    # -- Determine tables to scan for indexes
    tables_to_scan = []
    if not db_table:
        tables_to_scan = get_all_tables(conn)
    else:
        parsed_dbs_tables = parse_list_args(db_table)
        all_dbs = r.db_list().run(conn)

        for db, tables in parsed_dbs_tables.items():
            if db not in all_dbs:
                raise click.UsageError("Database '%s' not found." % db)
            if not tables:
                tables_to_scan.extend(get_all_tables(conn, [db]))
            else:
                all_tables_in_db = r.db(db).table_list().run(conn)
                for table in tables:
                    if table not in all_tables_in_db:
                        raise click.UsageError("Table '%s.%s' not found." % (db, table))
                    tables_to_scan.append((db, table))

    # -- Find all indexes to rebuild
    indexes_to_rebuild = []
    for db, table in tables_to_scan:
        query = r.db(db).table(table).index_status()
        if not force:
            query = query.filter({"outdated": True})

        statuses = query.run(conn)
        for status in statuses:
            indexes_to_rebuild.append((db, table, status["index"]))

    if not indexes_to_rebuild:
        if force:
            click.echo("No indexes to rebuild.")
        else:
            click.echo("No outdated indexes to rebuild.")
        return

    total_indexes = len(indexes_to_rebuild)
    click.echo("Rebuilding %d index(es)..." % total_indexes)

    conn.close()  # Close main connection, workers will use their own

    # -- Start rebuilding
    progress_state = {"indexes_done": 0, "lock": threading.Lock()}

    with ThreadPoolExecutor(max_workers=concurrent) as executor:
        futures = {
            executor.submit(
                rebuild_worker, db, table, index, connection_args, progress_state
            ): (db, table, index)
            for db, table, index in indexes_to_rebuild
        }

        while any(not future.done() for future in futures):
            print_progress(
                progress_state["indexes_done"], total_indexes, prefix="Rebuilding"
            )
            threading.Event().wait(0.1)

        print_progress(
            progress_state["indexes_done"], total_indexes, prefix="Rebuilding"
        )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                db, table, index = futures[future]
                click.echo(
                    "\nError rebuilding index %s.%s.%s: %s" % (db, table, index, e),
                    err=True,
                )

    click.echo("\nIndex rebuild complete.")

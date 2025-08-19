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
CLI utility functions for RethinkDB Python CLI commands.
"""

import base64
import getpass
import logging
from typing import Dict, List, Optional

import click

from rethinkdb import r
from rethinkdb.ast import RqlBinary, RqlQuery
from rethinkdb.errors import ReqlDriverError


def get_logger():
    """Get the CLI logger."""
    return logging.getLogger("rethinkdb.cli")


def print_progress(current: int, total: int, prefix: str = "Progress") -> None:
    """Print a simple progress bar using logging."""
    logger = get_logger()
    percent = int((current / total) * 100) if total else 100
    bar = "#" * (percent // 2) + "-" * (50 - percent // 2)

    # Use info level for progress updates
    # Don't use \r with logging as it doesn't work well with timestamps
    logger.info("%s: |%s| %d%% (%d/%d)", prefix, bar, percent, current, total)

    # No need for extra newline since logging handles line breaks


def parse_list_args(args: List[str]) -> Dict[str, List[str]]:
    """Parse list args into a dict of db: [tables] format."""
    db_tables: Dict[str, List[str]] = {}

    for arg in args:
        if "." in arg:
            db, table = arg.split(".", 1)
            db_tables.setdefault(db, []).append(table)
        else:
            db_tables[arg] = []

    return db_tables


def json_default(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, RqlQuery):
        return obj.build()
    if isinstance(obj, RqlBinary):
        return {
            "$reql_type$": "BINARY",
            "data": base64.b64encode(obj).decode("utf-8"),
        }
    return str(obj)


def common_options(func):
    """Decorator to add common CLI options to a click command."""
    options = [
        click.option(
            "-q",
            "--quiet",
            is_flag=True,
            default=False,
            help="Suppress non-error messages.",
        ),
        click.option(
            "--debug",
            is_flag=True,
            default=False,
            help="Show debug output.",
        ),
        click.option(
            "-c",
            "--connect",
            "connect",
            default="localhost:28015",
            show_default=True,
            metavar="HOST:PORT",
            help="Host and port to connect to.",
        ),
        click.option(
            "--driver-port",
            "driver_port",
            default=None,
            metavar="PORT",
            type=int,
            help="Driver port of a rethinkdb server (overrides port in --connect).",
        ),
        click.option(
            "--host-name",
            "host_name",
            default=None,
            metavar="HOST",
            help="Host of a rethinkdb server (overrides host in --connect).",
        ),
        click.option(
            "-u",
            "--user",
            "user",
            default="admin",
            metavar="USERNAME",
            help="User name to connect as.",
        ),
        click.option(
            "-p", "--password", is_flag=True, help="Prompt for admin password."
        ),
        click.option(
            "--password-file",
            type=click.Path(exists=True),
            help="Read admin password from file.",
        ),
        click.option(
            "--tls-cert", type=click.Path(exists=True), help="Path to TLS certificate."
        ),
    ]
    for option in reversed(options):
        func = option(func)

    return func


def get_connection(
    connect: str,
    driver_port: Optional[int],
    host_name: Optional[str],
    user: str,
    password: Optional[bool],
    password_file: Optional[str],
    tls_cert: Optional[str],
    quiet: bool,
    debug: bool,
):
    """
    Establish a RethinkDB connection using the provided parameters.
    Handles password prompt and file, TLS, and logs errors unless quiet is set.
    """
    logger = get_logger()

    host = host_name or connect.split(":")[0]
    port = driver_port or (int(connect.split(":")[1]) if ":" in connect else 28015)

    actual_password: Optional[str] = None
    if password_file:
        with open(password_file, "r", encoding="utf-8") as f:
            actual_password = f.readline().strip()
    elif password is True:  # Handle the case where -p/--password flag is used
        actual_password = getpass.getpass("Admin password: ")

    ssl = {"ca_certs": tls_cert} if tls_cert else None

    try:
        conn = r.connect(  # type: ignore[attr-defined]
            host=host, port=port, user=user, password=actual_password, ssl=ssl
        )
        if debug:
            logger.debug("Connected to RethinkDB at %s:%d as %s", host, port, user)

        return conn
    except ReqlDriverError as e:
        if not quiet:
            logger.error("Error connecting to RethinkDB: %s", e)

        raise


def should_quiet(quiet: bool, debug: bool) -> bool:
    """Return True if non-error output should be suppressed."""
    return quiet and not debug

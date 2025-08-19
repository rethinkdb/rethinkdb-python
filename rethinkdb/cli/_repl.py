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
REPL starts a REPL session for RethinkDB. Enter ReQL queries as Python code using the 'r' object.
"""
import code
import sys

import click

from rethinkdb import r
from rethinkdb.cli.utils import common_options, get_connection
from rethinkdb.errors import ReqlDriverError


@common_options
@click.command()
def cmd_repl(
    connect,
    quiet,
    debug,
    host_name,
    driver_port,
    user,
    password,
    password_file,
    tls_cert,
):
    """
    Start a REPL session for RethinkDB. Enter ReQL queries as Python code using the 'r' object.
    Example: r.db('test').table_list().run(conn)
    """

    if host_name:
        host = host_name
    else:
        if ":" in connect:
            host, port = connect.split(":", 1)
            port = int(port)
        else:
            host = connect
            port = 28015

    if driver_port is not None:
        port = driver_port

    try:
        conn = get_connection(
            connect=connect,
            driver_port=driver_port,
            host_name=host_name,
            user=user,
            password=password,
            password_file=password_file,
            tls_cert=tls_cert,
            quiet=quiet,
            debug=debug,
        )
    except ReqlDriverError as e:
        if not quiet:
            click.echo("Error connecting to RethinkDB: %s" % e, err=True)
        sys.exit(1)

    banner = (
        "RethinkDB Python REPL\n"
        f"Connected to {host}:{port}\n"
        "Type ReQL queries using the 'r' object.\n"
        "The connection is available as 'conn'.\n"
        "Type exit() or Ctrl-D to exit.\n"
    )

    local_vars = {"r": r, "conn": conn}
    console = code.InteractiveConsole(locals=local_vars)

    try:
        console.interact(banner=banner)
    except SystemExit:
        pass
    except Exception as e:
        click.echo("REPL exited with error: %s" % e, err=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass

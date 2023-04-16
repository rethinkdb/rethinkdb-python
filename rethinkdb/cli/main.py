#!/usr/bin/env python

# Copyright 2022 - present RethinkDB
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
The main command is a special group that is the root of the command tree.

This command creates a subcommand tree that with the following commands for
those cases when the rethinkdb binary is not installed:
    - dump
    - export
    - import
    - index_rebuild
    - repl
    - restore
"""

import click

from rethinkdb.cli import (
    cmd_dump,
    cmd_export,
    cmd_import,
    cmd_index_rebuild,
    cmd_repl,
    cmd_restore,
)


@click.group
@click.option(
    "--debug",
    default=False,
    help="Print debug information.",
    envvar="RETHINKDB_DEBUG",
    type=click.BOOL,
)
@click.option(
    "--user",
    "-u",
    default="admin",
    help="The RethinkDB user to connect with.",
    envvar="RETHINKDB_USER",
    type=click.STRING,
)
@click.option(
    "--host",
    "-h",
    default="localhost",
    help="The RethinkDB host to connect to.",
    envvar="RETHINKDB_HOSTNAME",
    type=click.STRING,
)
@click.option(
    "--port",
    "-p",
    default=28015,
    help="The RethinkDB driver port to connect to.",
    envvar="RETHINKDB_DRIVER_PORT",
    type=click.INT,
)
def cmd_main(*args, **kwargs):
    """
    Group of commands for the RethinkDB database.
    """


cmd_main.add_command(cmd_dump, "dump")
cmd_main.add_command(cmd_export, "export")
cmd_main.add_command(cmd_import, "import")
cmd_main.add_command(cmd_index_rebuild, "index_rebuild")
cmd_main.add_command(cmd_repl, "repl")
cmd_main.add_command(cmd_restore, "restore")

if __name__ == "__main__":
    cmd_main(auto_envvar_prefix="RETHINKDB")

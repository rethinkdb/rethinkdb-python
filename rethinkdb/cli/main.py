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

import logging
import sys

import click

from rethinkdb.cli import (
    cmd_dump,
    cmd_export,
    cmd_import,
    cmd_index_rebuild,
    cmd_repl,
    cmd_restore,
)


def setup_logging(debug: bool = False, quiet: bool = False):
    """Setup logging configuration for CLI commands."""
    # Configure logging format
    if debug:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        log_level = logging.DEBUG
    else:
        log_format = "%(message)s"
        log_level = logging.INFO if not quiet else logging.WARNING

    # Always log to sys.stdout for CLI output
    logging.basicConfig(
        level=log_level,
        format=log_format,
        stream=sys.stdout,
        force=True,
    )

    # Create logger for CLI
    logger = logging.getLogger("rethinkdb.cli")
    logger.setLevel(log_level)

    return logger


@click.group
def cmd_main():
    """
    Group of commands for the RethinkDB database.
    """
    pass


def main():
    """Main entry point that sets up logging and runs the CLI."""
    # Parse arguments to get debug/quiet flags early
    debug = "--debug" in sys.argv
    quiet = "--quiet" in sys.argv or "-q" in sys.argv

    # Setup logging
    setup_logging(debug=debug, quiet=quiet)

    # Run the CLI
    cmd_main()


cmd_main.add_command(cmd_dump, "dump")
cmd_main.add_command(cmd_export, "export")
cmd_main.add_command(cmd_import, "import")
cmd_main.add_command(cmd_index_rebuild, "index_rebuild")
cmd_main.add_command(cmd_repl, "repl")
cmd_main.add_command(cmd_restore, "restore")

if __name__ == "__main__":
    main()

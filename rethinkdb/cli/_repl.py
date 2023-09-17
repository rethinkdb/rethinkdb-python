#!/usr/bin/env python

# Copyright 2022 RethinkDB
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
"""
import click


@click.command
def cmd_repl():

    #Rebuild outdated secondary indexes.

    click.echo("repl command")
"""

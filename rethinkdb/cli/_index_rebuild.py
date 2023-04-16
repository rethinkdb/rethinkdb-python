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
Index rebuild recreates outdated secondary indexes in a cluster.

This should be used after upgrading to a newer version of rethinkdb. There
will be a notification in the web UI if any secondary indexes are out-of-date.
"""

import click


@click.command
@click.pass_context
def cmd_index_rebuild(ctx):
    """
    Rebuild outdated secondary indexes.
    """
    click.echo("index rebuild command")

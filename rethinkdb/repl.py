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
This module contains the REPL's helper class to manage the established connection
on the local thread.
"""

__all__ = ["Repl", "REPL_CONNECTION_ATTRIBUTE"]


import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from rethinkdb.net import Connection

REPL_CONNECTION_ATTRIBUTE: str = "conn"


class Repl:
    """
    REPL helper class to get, set and clear the connection on the local thread.
    """

    __slots__ = ("is_repl_active", "thread_data")

    def __init__(self) -> None:
        self.is_repl_active: bool = False
        self.thread_data: threading.local = threading.local()

    def get_connection(self) -> Optional["Connection"]:
        """
        Get connection object from local thread.
        """

        return getattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE, None)

    def set_connection(self, connection: "Connection") -> None:
        """
        Set connection on local thread and activate REPL.
        """

        self.is_repl_active = True
        setattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE, connection)

    def clear_connection(self) -> None:
        """
        Clear the local thread and deactivate REPL.
        """

        self.is_repl_active = False

        if hasattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE):
            delattr(self.thread_data, REPL_CONNECTION_ATTRIBUTE)

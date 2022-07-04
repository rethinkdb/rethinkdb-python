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
This module is the collection of error classes raised by the client.
"""

__all__ = [
    "InvalidHandshakeStateError",
    "QueryPrinter",
    "ReqlAuthError",
    "ReqlCompileError",
    "ReqlCursorEmpty",
    "ReqlDriverCompileError",
    "ReqlDriverError",
    "ReqlError",
    "ReqlInternalError",
    "ReqlNonExistenceError",
    "ReqlOpFailedError",
    "ReqlOpIndeterminateError",
    "ReqlOperationError",
    "ReqlPermissionError",
    "ReqlQueryLogicError",
    "ReqlResourceLimitError",
    "ReqlRuntimeError",
    "ReqlServerCompileError",
    "ReqlTimeoutError",
    "ReqlUserError",
]


from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from rethinkdb.ast import ReqlQuery


class QueryPrinter:
    """
    Helper class to print Query failures in a formatted was using carets.
    """

    def __init__(self, root: "ReqlQuery", frames: Optional[List[int]] = None) -> None:
        self.root = root
        self.frames: List[int] = frames or []

    @property
    def query(self) -> str:
        """
        Return the composed query.
        """

        return "".join(self.__compose_term(self.root))

    @property
    def carets(self) -> str:
        """
        Return the carets indicating the location of the failure for the query.
        """

        return "".join(self.__compose_carets(self.root, self.frames))

    def __compose_term(self, term: "ReqlQuery") -> List[str]:
        """
        Recursively compose the query term.
        """

        args: List[list] = [
            self.__compose_term(arg)
            for arg in term._args  # pylint: disable=protected-access
        ]

        kwargs: Dict[Union[str, int], List[str]] = {
            k: self.__compose_term(v) for k, v in term.kwargs.items()
        }

        return term.compose(args, kwargs)

    def __compose_carets(self, term: "ReqlQuery", frames: List[int]) -> List[str]:
        """
        Generate the carets for the query term which caused the error.
        """

        # If the length of the frames is zero, it means that the current frame
        # is responsible for the error.
        if len(frames) == 0:
            return ["^" for _ in self.__compose_term(term)]

        current_frame: int = frames.pop(0)

        args: List[List[str]] = [
            self.__compose_carets(arg, frames)
            if current_frame == i
            else self.__compose_term(arg)
            for i, arg in enumerate(term._args)  # pylint: disable=protected-access
        ]

        kwargs: Dict[Union[str, int], List[str]] = {}
        for key, value in term.kwargs.items():
            if current_frame == key:
                kwargs[key] = self.__compose_carets(value, frames)
            else:
                kwargs[key] = self.__compose_term(value)

        return ["^" if i == "^" else " " for i in term.compose(args, kwargs)]


class ReqlError(Exception):
    """
    Base RethinkDB Query Language Error.
    """

    # NOTE: frames are the backtrace details
    def __init__(
        self,
        message: str,
        term: Optional["ReqlQuery"] = None,
        frames: Optional[List[int]] = None,
    ) -> None:
        super().__init__(message)

        self.message: str = message
        self.term = term
        self.frames: Optional[List[int]] = frames
        self.__query_printer: Optional[QueryPrinter] = None

        if self.term is not None and self.frames is not None:
            self.__query_printer = QueryPrinter(self.term, self.frames)

    def __str__(self) -> str:
        """
        Return the string representation of the error
        """

        if self.__query_printer is None:
            return self.message

        message = self.message.rstrip(".")
        return f"{message} in:\n{self.__query_printer.query}\n{self.__query_printer.carets}"

    def __repr__(self) -> str:
        """
        Return the representation of the error class.
        """

        return f"<{self.__class__.__name__} instance: {str(self)} >"


class ReqlDriverError(ReqlError):
    """
    Exception representing the Python client related exceptions.
    """


class ReqlRuntimeError(ReqlError):
    """
    Exception representing a runtime issue within the Python client. The runtime error
    is within the client and not the database.
    """


class ReqlAuthError(ReqlDriverError):
    """
    The exception raised when the authentication was unsuccessful to the database
    server.
    """

    def __init__(
        self, message: str, host: Optional[str] = None, port: Optional[int] = None
    ):
        if host and port:
            message = f"Could not connect to {host}:{port}, {message}"
        elif host and port is None:
            raise ValueError("If host is set, you must set port as well")
        elif host is None and port:
            raise ValueError("If port is set, you must set host as well")

        super().__init__(message)


class ReqlOperationError(ReqlRuntimeError):
    """
    Exception indicates that the error happened due to availability issues.
    """


class ReqlCompileError(ReqlError):
    """
    Exception representing any kind of compilation error. A compilation error
    can be raised during parsing a Python primitive into a Reql primitive or even
    when the server cannot parse a Reql primitive, hence it returns an error.
    """


class ReqlDriverCompileError(ReqlCompileError):
    """
    Exception indicates that a Python primitive cannot be converted into a
    Reql primitive.
    """


class ReqlServerCompileError(ReqlCompileError):
    """
    Exception indicates that a Reql primitive cannot be parsed by the server, hence
    it returned an error.
    """


class ReqlCursorEmpty(Exception):
    """
    Base exception indicates that the cursor was empty.
    """

    def __init__(self):
        self.message = "Cursor is empty."
        super().__init__(self.message)


class ReqlInternalError(ReqlRuntimeError):
    """
    Exception indicates that some internal error happened on server side.
    """


class ReqlQueryLogicError(ReqlRuntimeError):
    """
    Exception indicates that the query is syntactically correct, but not it has some
    logical errors.
    """


class ReqlNonExistenceError(ReqlQueryLogicError):
    """
    Exception indicates an error related to the absence of an expected value.
    """


class ReqlOpFailedError(ReqlOperationError):
    """
    Exception indicates that REQL operation failed.
    """


class ReqlOpIndeterminateError(ReqlOperationError):
    """
    Exception indicates that it is unknown whether an operation failed or not.
    """


class ReqlPermissionError(ReqlRuntimeError):
    """
    Exception indicates that the connected user has no permission to execute the query.
    """


class ReqlResourceLimitError(ReqlRuntimeError):
    """
    Exception indicates that the server exceeded a resource limit (e.g. the array size limit).
    """


class ReqlTimeoutError(ReqlDriverError, TimeoutError):
    """
    Exception indicates that the request towards the server is timed out.
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        message = "Operation timed out."

        if host and port:
            message = f"Could not connect to {host}:{port}, {message}"
        elif host and port is None:
            raise ValueError("If host is set, you must set port as well")
        elif host is None and port:
            raise ValueError("If port is set, you must set host as well")

        super().__init__(message)


class ReqlUserError(ReqlRuntimeError):
    """
    Exception indicates that en error caused by `r.error` with arguments.
    """


class InvalidHandshakeStateError(ReqlDriverError):
    """
    Exception raised when the client entered a not existing state during connection handshake.
    """

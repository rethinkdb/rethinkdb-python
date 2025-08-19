#!/usr/bin/env python3

"""
A script to generate the `rethinkdb/__init__.pyi` stub file.

This script inspects the `rethinkdb` package and generates a `.pyi` file
for `rethinkdb/__init__.py`, including all dynamically assigned methods and attributes
from the `ast`, `errors`, `net`, and `query` modules.
"""

import inspect
import os
import sys
from typing import Any, Callable

# Add the project root to the path to allow importing from `rethinkdb`
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from rethinkdb import ast, errors, net, query  # noqa: E402

MODULES_TO_INSPECT = {
    "ast": ast,
    "errors": errors,
    "net": net,
    "query": query,
}

PYI_HEADER = """# pylint: disable-all
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# mypy: ignore-errors
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Type, Union
import datetime

from rethinkdb import ast as ast_module
from rethinkdb import errors as errors_module
from rethinkdb import net as net_module
from rethinkdb import query as query_module
from rethinkdb.ast import RqlBinary, RqlQuery, RqlTzinfo
from rethinkdb.errors import (
    InvalidHandshakeStateError,
    QueryPrinter,
    ReqlAuthError,
    ReqlCompileError,
    ReqlCursorEmpty,
    ReqlDriverCompileError,
    ReqlDriverError,
    ReqlError,
    ReqlInternalError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlOperationError,
    ReqlPermissionError,
    ReqlQueryLogicError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
    ReqlServerCompileError,
    ReqlTimeoutError,
    ReqlUserError,
)
from rethinkdb.handshake import BaseHandshake
from rethinkdb.net import Connection, Cursor, DefaultConnection


class RethinkDB:
    ast: ast_module
    errors: errors_module
    net: net_module
    query: query_module
    connection_type: Optional[Type[Connection]]

    def __init__(self) -> None: ...
    def set_loop_type(self, library: Optional[str] = None) -> None: ...
    def connect(self, *connect_args: Any, **kwargs: Any) -> Connection: ...
"""

PYI_FOOTER = """

r: RethinkDB
"""


def get_type_hint(obj: Any, name: str, module_name: str) -> str:
    """
    Generates a type hint string for a given object.
    This function contains special cases for objects that require specific type hints.
    """
    # Special cases
    if module_name == "ast":
        if name == "expr":
            return """Callable[
        [
            Union[
                str,
                bytes,
                RqlQuery,
                RqlBinary,
                datetime.date,
                datetime.datetime,
                Mapping[Any, Any],
                Iterable[Any],
                Callable[..., Any],
            ],
            int,
        ],
        RqlQuery,
    ]"""

    if module_name == "net":
        if name == "make_connection":
            return """Callable[
        [
            Type[Connection],
            str,
            int,
            Optional[str],
            str,
            Optional[str],
            int,
            Optional[Dict[str, Any]],
            Optional[str],
            Type[BaseHandshake],
        ],
        Connection,
    ]"""

    if module_name == "query":
        if name == "binary":
            return "Callable[[bytes], RqlQuery]"
        if name == "make_timezone":
            return "Callable[..., RqlTzinfo]"

    # General cases
    if inspect.isfunction(obj):
        if module_name == "query":
            import re, textwrap

            try:
                src = textwrap.dedent(inspect.getsource(obj))
            except (OSError, TypeError):
                src = ""

            m = re.search(r"return\s+ast\.([A-Za-z_][A-Za-z0-9_]*)", src)
            if m:
                cls = m.group(1)
                return f"Callable[..., ast_module.{cls}]"

            return "Callable[..., RqlQuery]"
        return "Callable[..., Any]"

    if inspect.isclass(obj):
        return f"Type[{name}]"

    if isinstance(obj, (query.ReqlConstant, ast.ImplicitVar)):
        return "RqlQuery"

    if isinstance(obj, int):
        return "int"

    return "Any"


def generate_pyi_content() -> str:
    """
    Generates the full content of the __init__.pyi file.
    """
    content = [PYI_HEADER]

    for module_name, module in MODULES_TO_INSPECT.items():
        # blank line before each section
        content.append("")
        content.append(f"    # from rethinkdb.{module_name}")
        if not hasattr(module, "__all__"):
            continue

        for name in sorted(module.__all__):
            obj = getattr(module, name, None)
            if obj is not None:
                type_hint = get_type_hint(obj, name, module_name)
                docstring = inspect.getdoc(obj)

                if docstring:
                    import textwrap

                    # Keep only the summary and first paragraph to avoid bloating the stub
                    lines = textwrap.dedent(docstring).strip().split("\n")
                    # Capture up to first blank line after summary
                    summary_lines = []
                    for ln in lines:
                        if ln.strip() == "":
                            break
                        summary_lines.append(ln)

                    summary = "\n".join(summary_lines)

                    indented_doc = textwrap.indent(summary, " " * 4)

                    content.append(f"    {name}: {type_hint} = ...")
                    content.append("    \"\"\"")
                    content.append(indented_doc)
                    content.append("    \"\"\"")
                else:
                    content.append(f"    {name}: {type_hint} = ...")

    content.append(PYI_FOOTER)
    # Join with newlines ensuring blank line between each top-level list entry
    return "\n".join(content)


def main() -> None:
    """
    Main function to generate and write the `__init__.pyi` file.
    """
    pyi_path = os.path.join(project_root, "rethinkdb", "__init__.pyi")
    content = generate_pyi_content()

    with open(pyi_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Successfully generated {pyi_path}")


if __name__ == "__main__":
    main()

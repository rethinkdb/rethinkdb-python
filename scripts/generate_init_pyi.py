#!/usr/bin/env python3
"""
Script to auto-generate rethinkdb/__init__.pyi for IDE and linter support.
"""
import importlib
import os
import inspect
from typing import Any, Set, Dict

MODULES = [
    ("rethinkdb.net", "Type"),
    ("rethinkdb.query", "Any"),
    ("rethinkdb.ast", "Any"),
    ("rethinkdb.errors", "Type"),
]

HEADER_BASE = """from typing import {imports}
{extra_imports}

class RethinkDB:
"""

FOOTER = '''    # Methods
    def set_loop_type(self, loop_type: str) -> None: ...

r: RethinkDB
'''

INDENT = "    "

used_typing = set()
used_imports: Dict[str, Set[str]] = {}
used_stdlib: Dict[str, Set[str]] = {}  # e.g., {"datetime": {"date", "datetime"}}
used_collections_abc: Set[str] = set()

def add_used_import(module: str, name: str):
    if module == "builtins":
        return
    if module == "typing":
        used_typing.add(name)
        return
    if module == "datetime":
        if "datetime" not in used_stdlib:
            used_stdlib["datetime"] = set()
        used_stdlib["datetime"].add(name)
        return
    if module == "collections.abc":
        used_collections_abc.add(name)
        return
    if module.startswith("rethinkdb."):
        if module not in used_imports:
            used_imports[module] = set()
        used_imports[module].add(name)
        return
    # fallback for other stdlib or 3rd party
    if module not in used_imports:
        used_imports[module] = set()
    used_imports[module].add(name)

def get_qualified_type_str(annot):
    # Handle typing constructs (e.g., List[ReqlConstant])
    if hasattr(annot, "__origin__") and annot.__origin__ is not None:
        origin = annot.__origin__
        args = getattr(annot, "__args__", ())
        if hasattr(origin, "_name") and origin._name:
            add_used_import(getattr(origin, "__module__", "typing"), origin._name)
            origin_name = origin._name
        else:
            origin_name = get_qualified_type_str(origin)
        if args:
            args_str = ", ".join(get_qualified_type_str(a) for a in args)
            return f"{origin_name}[{args_str}]"
        else:
            return origin_name
    # Handle built-in types and Any
    if annot is Any or annot is inspect.Parameter.empty or annot is inspect.Signature.empty:
        used_typing.add("Any")
        return "Any"
    # Handle typing module types (e.g., typing.List)
    if hasattr(annot, "_name") and annot._name and getattr(annot, "__module__", None) == "typing":
        used_typing.add(annot._name)
        return annot._name
    # Handle built-in types
    if hasattr(annot, "__module__") and annot.__module__ == "builtins":
        return annot.__name__
    # Handle classes
    if hasattr(annot, "__module__") and hasattr(annot, "__name__"):
        add_used_import(annot.__module__, annot.__name__)
        return annot.__name__
    # Fallback to str
    return str(annot)

def main():
    lines = []
    seen = set()
    for module_name, type_hint in MODULES:
        mod = importlib.import_module(module_name)
        exports = getattr(mod, "__all__", [])
        for name in exports:
            if name in seen:
                continue
            seen.add(name)
            obj = getattr(mod, name, None)
            if obj is None:
                continue
            if inspect.isclass(obj):
                used_typing.add("Type")
                add_used_import(obj.__module__, obj.__name__)
                lines.append(f"{INDENT}{name}: Type[{obj.__name__}]\n")
            elif inspect.isfunction(obj):
                # Write the function signature
                try:
                    sig = inspect.signature(obj)
                    # Separate non-default and default params
                    non_default_params = []
                    default_params = []
                    for pname, param in sig.parameters.items():
                        annot_str = get_qualified_type_str(param.annotation)
                        param_str = f"{pname}: {annot_str}"
                        if param.default is not inspect.Parameter.empty:
                            param_str += " = ..."
                            default_params.append(param_str)
                        else:
                            non_default_params.append(param_str)
                    params = non_default_params + default_params
                    ret_str = get_qualified_type_str(sig.return_annotation)
                    lines.append(f"{INDENT}def {name}({', '.join(params)}) -> {ret_str}: ...\n")
                except Exception:
                    used_typing.add("Any")
                    lines.append(f"{INDENT}def {name}(*args, **kwargs) -> Any: ...\n")
            else:
                # Variable/constant: use fully qualified class name
                cls = obj.__class__
                if cls.__module__ == "builtins":
                    type_str = cls.__name__
                elif cls.__module__ == "typing":
                    used_typing.add(cls.__name__)
                    type_str = cls.__name__
                else:
                    add_used_import(cls.__module__, cls.__name__)
                    type_str = cls.__name__
                lines.append(f"{INDENT}{name}: {type_str}\n")
    # Compose extra imports for all used non-builtin, non-typing types
    extra_imports = []
    # stdlib: datetime
    if used_stdlib.get("datetime"):
        extra_imports.append(f"from datetime import {', '.join(sorted(used_stdlib['datetime']))}")
    # stdlib: collections.abc
    if used_collections_abc:
        extra_imports.append(f"from collections.abc import {', '.join(sorted(used_collections_abc))}")
    # rethinkdb and other modules
    for module, names in sorted(used_imports.items()):
        if module.startswith("rethinkdb.") or module in ("datetime", "collections.abc"):
            import_line = f"from {module} import {', '.join(sorted(names))}"
            extra_imports.append(import_line)
    extra_imports_str = "\n".join(extra_imports)
    # Compose header with only used typing imports
    imports = ", ".join(sorted(used_typing)) if used_typing else ""
    header = HEADER_BASE.format(imports=imports, extra_imports=extra_imports_str)
    lines.insert(0, header)
    lines.append(FOOTER)
    out_path = os.path.join(os.path.dirname(__file__), "..", "rethinkdb", "__init__.pyi")
    with open(out_path, "w") as f:
        f.writelines(lines)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()

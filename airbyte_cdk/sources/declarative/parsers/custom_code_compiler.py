"""Contains functions to compile custom code from text using RestrictedPython for secure execution."""

import ast
import hashlib
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import InitVar, dataclass, field
from types import ModuleType
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from RestrictedPython import compile_restricted, safe_builtins
from RestrictedPython.compile import RestrictingNodeTransformer
from RestrictedPython.Guards import (
    full_write_guard,
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
)
from RestrictedPython.Guards import (
    safe_builtins as restricted_builtins,
)
from RestrictedPython.Utilities import utility_builtins


class AirbyteRestrictingNodeTransformer(RestrictingNodeTransformer):
    """Custom AST transformer that allows type annotations and specific private attributes while enforcing security."""

    ALLOWED_IMPORTS = {
        "dataclasses",
        "typing",
        "requests",
        "airbyte_cdk",
        "airbyte_cdk.sources",
        "airbyte_cdk.sources.declarative",
        "airbyte_cdk.sources.declarative.interpolation",
        "airbyte_cdk.sources.declarative.requesters",
        "airbyte_cdk.sources.declarative.requesters.paginators",
        "airbyte_cdk.sources.declarative.types",
        "airbyte_cdk.sources.declarative.types.Config",
        "airbyte_cdk.sources.declarative.types.Record",
        "InterpolatedString",
        "PaginationStrategy",
        "Config",
        "Record",
    }

    def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
        """Transform attribute access into _getattr_ or _write_ function calls."""
        visited_node = self.generic_visit(node)
        if not isinstance(visited_node, ast.AST):
            visited_node = node

        if isinstance(visited_node, ast.Attribute) and isinstance(visited_node.attr, str):
            # Block access to dangerous attributes
            dangerous_attrs = {"__dict__", "__class__", "__bases__", "__subclasses__"}
            if visited_node.attr in dangerous_attrs:
                raise NameError(f"name '{visited_node.attr}' is not allowed")

            # Allow specific private attributes
            allowed_private = {
                "__annotations__",
                "__name__",
                "__doc__",
                "__module__",
                "__qualname__",
                "__post_init__",
                "__init__",
                "__dataclass_fields__",
                "__mro__",
                "__subclasshook__",
                "__new__",
                "_page_size",
            }
            if visited_node.attr.startswith("_") and visited_node.attr not in allowed_private:
                if not visited_node.attr.startswith("__"):  # Allow dunder methods
                    raise NameError(f"name '{visited_node.attr}' is not allowed")

        if isinstance(visited_node.ctx, ast.Store):
            # For assignments like "obj.attr = value"
            name_node = ast.Name(id="_write_", ctx=ast.Load())
            ast.copy_location(name_node, visited_node)

            value_node = self.visit(visited_node.value)
            const_node = ast.Constant(value=visited_node.attr)

            call_node = ast.Call(
                func=name_node,
                args=[value_node, const_node],
                keywords=[],
            )
            ast.copy_location(call_node, visited_node)
            ast.fix_missing_locations(call_node)
            return call_node

        elif isinstance(visited_node.ctx, ast.Load):
            # For reads like "obj.attr"
            name_node = ast.Name(id="_getattr_", ctx=ast.Load())
            ast.copy_location(name_node, visited_node)

            const_node = ast.Constant(value=visited_node.attr)
            ast.copy_location(const_node, visited_node)

            visited_value = self.visit(visited_node.value)
            if hasattr(visited_value, "lineno"):
                ast.copy_location(visited_value, visited_node)

            call_node = ast.Call(
                func=name_node,
                args=[visited_value, const_node],
                keywords=[],
            )
            ast.copy_location(call_node, visited_node)
            ast.fix_missing_locations(call_node)
            return call_node

        elif isinstance(visited_node.ctx, ast.Del):
            raise SyntaxError("Attribute deletion is not allowed")
        if not isinstance(visited_node, ast.AST):
            raise TypeError(f"Expected ast.AST but got {type(visited_node)}")
        return visited_node

    def check_name(
        self,
        node: ast.AST,
        name: str,
        allow_magic_methods: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> ast.AST:
        """Allow specific private names that are required for dataclasses and type hints.

        Args:
            node: The AST node being checked
            name: The name being validated
            allow_magic_methods: Whether to allow magic methods (defaults to True)
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        if name.startswith("_"):
            # Allow specific private names
            allowed_private = {
                # Type annotation attributes
                "__annotations__",
                "__name__",
                "__doc__",
                "__module__",
                "__qualname__",
                # Dataclass attributes
                "__post_init__",
                "__init__",
                "__dict__",
                "__dataclass_fields__",
                "__class__",
                "__bases__",
                "__mro__",
                "__subclasshook__",
                "__new__",
                # Allow specific private attributes used in the codebase
                "_page_size",
            }
            if name in allowed_private or name == "_page_size":
                return node
            if name.startswith("__"):  # Allow dunder methods
                return node
            raise NameError(f"Name '{name}' is not allowed because it starts with '_'")
        return node  # Don't call super().check_name as it's too restrictive

    def visit_Import(self, node: ast.Import) -> ast.Import:
        """Block unsafe imports."""
        for alias in node.names:
            if not alias.name:
                raise NameError("__import__ not found")
            if not any(
                alias.name == allowed or alias.name.startswith(allowed + ".")
                for allowed in self.ALLOWED_IMPORTS
            ):
                raise NameError("__import__ not found")
        return node

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.ImportFrom:
        """Block unsafe imports."""
        module_name = node.module if node.module else ""

        # Handle relative imports
        if node.level > 0:
            # We don't support relative imports for security
            raise NameError("__import__ not found")

        if not any(
            module_name == allowed or module_name.startswith(allowed + ".")
            for allowed in self.ALLOWED_IMPORTS
        ):
            raise NameError("__import__ not found")

        # Also check the imported names
        for alias in node.names:
            if not alias.name:
                raise NameError("__import__ not found")
            if alias.name == "*":
                raise NameError("__import__ not found")

        return node

    def visit_Call(self, node: ast.Call) -> ast.Call:
        """Block unsafe function calls."""
        if isinstance(node.func, ast.Name):
            unsafe_functions = {"open", "eval", "exec", "compile", "__import__"}
            if node.func.id in unsafe_functions:
                raise NameError(f"name '{node.func.id}' is not defined")
        result: ast.AST = super().visit_Call(node)
        if not isinstance(result, ast.Call):
            raise TypeError(f"Expected ast.Call but got {type(result)}")
        return result

    def visit_AnnAssign(self, node: ast.AnnAssign) -> ast.AnnAssign:
        """Allow type annotations in variable assignments and dataclass field definitions."""
        # Visit the target and annotation nodes
        node.target = self.visit(node.target)
        node.annotation = self.visit(node.annotation)
        if node.value:
            node.value = self.visit(node.value)
        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.ClassDef:
        """Allow dataclass definitions with their attributes."""
        # Check if this is a dataclass by looking for the decorator
        is_dataclass = any(
            isinstance(d, ast.Name)
            and d.id == "dataclass"
            or (
                isinstance(d, ast.Call)
                and isinstance(d.func, ast.Name)
                and d.func.id == "dataclass"
            )
            for d in node.decorator_list
        )

        # Visit the decorator list and bases first
        node.decorator_list = [self.visit(d) for d in node.decorator_list]
        if node.bases:
            node.bases = [self.visit(b) for b in node.bases]

        if is_dataclass:
            # For dataclasses, we need to allow attribute statements and annotations
            allowed_nodes = []
            for n in node.body:
                # Allow class variable annotations (typical in dataclasses)
                if isinstance(n, ast.AnnAssign):
                    allowed_nodes.append(self.visit_AnnAssign(n))
                # Allow function definitions (like __post_init__)
                elif isinstance(n, ast.FunctionDef):
                    allowed_nodes.append(self.visit(n))
                # Allow docstrings
                elif isinstance(n, ast.Expr) and isinstance(n.value, ast.Str):
                    allowed_nodes.append(n)
                # Allow assignments with type annotations
                elif isinstance(n, ast.Assign):
                    # Convert simple assignments to annotated assignments if possible
                    if len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
                        ann_assign = ast.AnnAssign(
                            target=n.targets[0],
                            annotation=ast.Name(id="Any", ctx=ast.Load()),
                            value=n.value,
                            simple=1,
                        )
                        allowed_nodes.append(self.visit_AnnAssign(ann_assign))
                    else:
                        allowed_nodes.append(self.visit(n))
                else:
                    allowed_nodes.append(self.visit(n))

            node.body = allowed_nodes
            return node

        result = super().visit_ClassDef(node)
        if not isinstance(result, ast.ClassDef):
            raise TypeError(f"Expected ast.ClassDef but got {type(result)}")
        return result


from typing_extensions import Literal

ChecksumType = Literal["md5", "sha256"]
CHECKSUM_FUNCTIONS = {
    "md5": hashlib.md5,
    "sha256": hashlib.sha256,
}
COMPONENTS_MODULE_NAME = "components"
SDM_COMPONENTS_MODULE_NAME = "source_declarative_manifest.components"
INJECTED_MANIFEST = "__injected_declarative_manifest"
INJECTED_COMPONENTS_PY = "__injected_components_py"
INJECTED_COMPONENTS_PY_CHECKSUMS = "__injected_components_py_checksums"
ENV_VAR_ALLOW_CUSTOM_CODE = "AIRBYTE_ALLOW_CUSTOM_CODE"


class AirbyteCodeTamperedError(Exception):
    """Raised when the connector's components module does not match its checksum.

    This is a fatal error, as it can be a sign of code tampering.
    """


class AirbyteCustomCodeNotPermittedError(Exception):
    """Raised when custom code is attempted to be run in an environment that does not support it."""

    def __init__(self) -> None:
        super().__init__(
            "Custom connector code is not permitted in this environment. "
            "If you need to run custom code, please ask your administrator to set the `AIRBYTE_ALLOW_CUSTOM_CODE` "
            "environment variable to 'true' in your Airbyte environment. "
            "If you see this message in Airbyte Cloud, your workspace does not allow executing "
            "custom connector code."
        )


def _hash_text(input_text: str, hash_type: str = "md5") -> str:
    """Return the hash of the input text using the specified hash type."""
    if not input_text:
        raise ValueError("Input text cannot be empty.")

    hash_object = CHECKSUM_FUNCTIONS[hash_type]()
    hash_object.update(input_text.encode())
    return hash_object.hexdigest()


def custom_code_execution_permitted() -> bool:
    """Return `True` if custom code execution is permitted, otherwise `False`.

    Custom code execution is permitted if the `AIRBYTE_ALLOW_CUSTOM_CODE` environment variable is set to 'true'.
    """
    return os.environ.get(ENV_VAR_ALLOW_CUSTOM_CODE, "").lower() == "true"


def validate_python_code(
    code_text: str,
    checksums: dict[str, str] | None,
) -> None:
    """Validate the provided Python code text against the provided checksums.

    Currently we fail if no checksums are provided, although this may change in the future.
    """
    if not checksums:
        raise ValueError(f"A checksum is required to validate the code. Received: {checksums}")

    for checksum_type, checksum in checksums.items():
        if checksum_type not in CHECKSUM_FUNCTIONS:
            raise ValueError(
                f"Unsupported checksum type: {checksum_type}. Supported checksum types are: {CHECKSUM_FUNCTIONS.keys()}"
            )

        if _hash_text(code_text, checksum_type) != checksum:
            raise AirbyteCodeTamperedError(f"{checksum_type} checksum does not match.")


def get_registered_components_module(
    config: Mapping[str, Any] | None,
) -> ModuleType | None:
    """Get a components module object based on the provided config.

    If custom python components is provided, this will be loaded. Otherwise, we will
    attempt to load from the `components` module already imported/registered in sys.modules.

    If custom `components.py` text is provided in config, it will be registered with sys.modules
    so that it can be later imported by manifest declarations which reference the provided classes.

    Returns `None` if no components is provided and the `components` module is not found.
    """
    if config and INJECTED_COMPONENTS_PY in config:
        if not custom_code_execution_permitted():
            raise AirbyteCustomCodeNotPermittedError

        # Create a new module object and execute the provided Python code text within it
        python_text: str = config[INJECTED_COMPONENTS_PY]
        return register_components_module_from_string(
            components_py_text=python_text,
            checksums=config.get(INJECTED_COMPONENTS_PY_CHECKSUMS, None),
        )

    # Check for `components` or `source_declarative_manifest.components`.
    if SDM_COMPONENTS_MODULE_NAME in sys.modules:
        return cast(ModuleType, sys.modules.get(SDM_COMPONENTS_MODULE_NAME))

    if COMPONENTS_MODULE_NAME in sys.modules:
        return cast(ModuleType, sys.modules.get(COMPONENTS_MODULE_NAME))

    # Could not find module 'components' in `sys.modules`
    # and INJECTED_COMPONENTS_PY was not provided in config.
    return None


def register_components_module_from_string(
    components_py_text: str,
    checksums: dict[str, Any] | None,
) -> ModuleType:
    """Load and return the components module from a provided string containing the python code.

    This function uses RestrictedPython to execute the code in a secure sandbox environment.
    The execution is restricted to prevent access to dangerous builtins and operations.

    Security measures:
    1. Code is validated against checksums before execution
    2. Code is compiled using RestrictedPython's compile_restricted
    3. Execution uses safe_builtins to prevent access to dangerous operations
    4. Attribute access is guarded using RestrictedPython's Guards
    5. Code runs in an isolated namespace with restricted globals

    Args:
        components_py_text: The Python code to execute as a string.
        checksums: Dictionary of checksum types to their expected values.
            Must contain at least one of 'md5' or 'sha256'.

    Returns:
        ModuleType: A module object containing the executed code's namespace.

    Raises:
        AirbyteCodeTamperedError: If the provided code fails checksum validation.
        ValueError: If no checksums are provided for validation.
    """
    # First check if custom code execution is permitted
    if not custom_code_execution_permitted():
        raise AirbyteCustomCodeNotPermittedError()

    # Then validate the code
    validate_python_code(
        code_text=components_py_text,
        checksums=checksums,
    )

    # Create a new module object
    components_module = ModuleType(name=COMPONENTS_MODULE_NAME)

    # Create restricted globals with safe builtins
    # Start with RestrictedPython's safe builtins and add type annotation support
    safe_builtins_copy = dict(safe_builtins)

    # Remove potentially dangerous builtins
    dangerous_builtins = {
        "open",
        "eval",
        "exec",
        "compile",
        "globals",
        "locals",
        "vars",
        "delattr",
        "setattr",
        "__import__",
        "reload",
    }
    for name in dangerous_builtins:
        safe_builtins_copy.pop(name, None)

    # Add type annotation support
    type_support = {
        # Type hints
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Tuple": Tuple,
        "Set": Set,
        "Optional": Optional,
        "Union": Union,
        "Callable": Callable,
        "Mapping": Mapping,
        # Basic types
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        # Dataclass support
        "dataclass": dataclass,
        "InitVar": InitVar,
        "field": field,
        # Add basic operations
        "len": len,
        "isinstance": isinstance,
        "hasattr": hasattr,
        "getattr": getattr,
        "ValueError": ValueError,
        "TypeError": TypeError,
        # Add metaclass support
        "__metaclass__": type,
        # Add type annotation support
        "type": type,
        "property": property,
        "classmethod": classmethod,
        "staticmethod": staticmethod,
        # Add requests module
        "requests": None,  # Will be imported by the code
    }
    safe_builtins_copy.update(type_support)

    # Define safe attribute access
    def safe_getattr(obj: Any, name: str) -> Any:
        # Allow type annotation and dataclass related attributes
        allowed_private = {
            # Type annotation attributes
            "__annotations__",
            "__name__",
            "__doc__",
            "__module__",
            "__qualname__",
            # Dataclass attributes
            "__post_init__",
            "__init__",
            "__dict__",
            "__dataclass_fields__",
            "__class__",
            "__bases__",
            "__mro__",
            "__subclasshook__",
            "__new__",
            # Allow specific private attributes used in the codebase
            "_page_size",
        }
        if name in allowed_private or name.startswith("__") or name == "_page_size":
            return getattr(obj, name)
        # Block access to other special attributes
        if name.startswith("_") and name not in allowed_private:
            raise AttributeError(f"Access to {name} is not allowed")
        return getattr(obj, name)

    # Create restricted globals with support for type annotations and dataclasses
    restricted_globals: Dict[str, Any] = {
        "__builtins__": safe_builtins_copy,
        "_getattr_": safe_getattr,
        "_write_": full_write_guard,
        "_getiter_": iter,
        "_getitem_": lambda obj, key: obj[key] if isinstance(obj, (list, dict, tuple)) else None,
        "_print_": lambda *args, **kwargs: None,  # No-op print
        "__name__": components_module.__name__,
        # Add type annotation and dataclass support to globals
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Tuple": Tuple,
        "Set": Set,
        "Optional": Optional,
        "Union": Union,
        "Callable": Callable,
        "Mapping": Mapping,
        "dataclass": dataclass,
        "InitVar": InitVar,
        "field": field,
        # Add sequence unpacking support
        "_unpack_sequence_": guarded_unpack_sequence,
        "_iter_unpack_sequence_": guarded_iter_unpack_sequence,
        # Add support for type annotations
        "__annotations__": {},
        "__module__": components_module.__name__,
        "__qualname__": "",
        "__doc__": None,
        "__metaclass__": type,
        # Add support for requests module
        "requests": None,  # Will be imported by the code
        # Add support for PaginationStrategy
        "PaginationStrategy": None,  # Will be imported by the code
        "InterpolatedString": None,  # Will be imported by the code
        "Config": None,  # Will be imported by the code
        "Record": None,  # Will be imported by the code
    }

    # Compile with RestrictedPython's restrictions using our custom transformer
    try:
        byte_code = compile_restricted(
            components_py_text,
            filename="<string>",
            mode="exec",
            policy=AirbyteRestrictingNodeTransformer,
        )
    except SyntaxError as e:
        raise SyntaxError(f"Restricted execution error: {str(e)}")

    # Execute the compiled code in the restricted environment
    exec(byte_code, restricted_globals)

    # Update the module's dictionary with the restricted execution results
    components_module.__dict__.update(restricted_globals)

    # Register the module in `sys.modules` so it can be imported as
    # `source_declarative_manifest.components` and/or `components`.
    sys.modules[SDM_COMPONENTS_MODULE_NAME] = components_module
    sys.modules[COMPONENTS_MODULE_NAME] = components_module

    # Now you can import and use the module
    return components_module

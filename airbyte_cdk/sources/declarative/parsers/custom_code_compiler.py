"""Contains functions to compile custom code from text using RestrictedPython for secure execution."""

import hashlib
import os
import sys
from collections.abc import Mapping
from types import ModuleType
from typing import Any, cast, Dict

from RestrictedPython import compile_restricted, safe_builtins
from RestrictedPython.Guards import guarded_getattr, guarded_setattr, guarded_iter_unpack, guarded_unpack_sequence
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
    # First validate the code
    validate_python_code(
        code_text=components_py_text,
        checksums=checksums,
    )

    # Create a new module object
    components_module = ModuleType(name=COMPONENTS_MODULE_NAME)

    # Create restricted globals with safe builtins and guards
    restricted_globals: Dict[str, Any] = {
        "__builtins__": safe_builtins,
        "_getattr_": guarded_getattr,
        "_setattr_": guarded_setattr,
        "_iter_unpack_sequence_": guarded_iter_unpack,
        "_unpack_sequence_": guarded_unpack_sequence,
        "__name__": components_module.__name__,
    }

    # Compile the code using RestrictedPython
    byte_code = compile_restricted(
        components_py_text,
        filename="<string>",
        mode="exec",
    )

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

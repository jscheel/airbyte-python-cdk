"""Unit tests for custom code compiler with RestrictedPython security features."""

import os
from typing import Any, Dict
from unittest.mock import patch

import pytest

from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    AirbyteCodeTamperedError,
    AirbyteCustomCodeNotPermittedError,
    register_components_module_from_string,
    validate_python_code,
)


def test_validate_python_code_with_valid_md5():
    """Test that code validation passes with correct MD5 checksum."""
    code = "def test(): return 'hello'"
    checksums = {"md5": "8901edeabbb26c1d0496c2c38a95cf17"}
    validate_python_code(code, checksums)  # Should not raise


def test_validate_python_code_with_valid_sha256():
    """Test that code validation passes with correct SHA256 checksum."""
    code = "def test(): return 'hello'"
    checksums = {"sha256": "bc379fc64b5ea5d0bb232194b4ce6be0bc16de0d30b33c069a2d63eb911e74b0"}
    validate_python_code(code, checksums)  # Should not raise


def test_validate_python_code_with_invalid_checksum():
    """Test that code validation fails with incorrect checksum."""
    code = "def test(): return 'hello'"
    checksums = {"md5": "invalid"}
    with pytest.raises(AirbyteCodeTamperedError):
        validate_python_code(code, checksums)


def test_validate_python_code_with_no_checksums():
    """Test that code validation fails when no checksums are provided."""
    code = "def test(): return 'hello'"
    with pytest.raises(ValueError, match="A checksum is required"):
        validate_python_code(code, None)


def test_register_components_module_safe_code():
    """Test that safe code executes successfully in restricted environment."""
    code = """
def get_value():
    return 42

def add_numbers(a, b):
    return a + b
"""
    checksums = {"md5": "8c00db73237e4ba737003dc78fc5e63f"}
    with patch.dict(os.environ, {"AIRBYTE_ALLOW_CUSTOM_CODE": "true"}):
        module = register_components_module_from_string(code, checksums)
        assert module.get_value() == 42
        assert module.add_numbers(2, 3) == 5


def test_register_components_module_unsafe_imports():
    """Test that unsafe module imports are blocked."""
    code = """
import os
def delete_file():
    os.remove('/tmp/test')
"""
    checksums = {"md5": "a40d238c0ae2c750df62a45bbaf344a2"}
    with patch.dict(os.environ, {"AIRBYTE_ALLOW_CUSTOM_CODE": "true"}):
        with pytest.raises(Exception) as exc_info:
            register_components_module_from_string(code, checksums)
        error_msg = str(exc_info.value)
        assert any(
            msg in error_msg for msg in ["__import__ not found", "name '__import__' is not defined"]
        )


def test_register_components_module_unsafe_builtins():
    """Test that unsafe builtin operations are blocked."""
    code = """
def evil_code():
    open('/etc/passwd', 'r').read()
"""
    checksums = {"md5": "62c7d65594a5b7654ff6456125483c05"}
    with patch.dict(os.environ, {"AIRBYTE_ALLOW_CUSTOM_CODE": "true"}):
        with pytest.raises((NameError, AttributeError)) as exc_info:
            module = register_components_module_from_string(code, checksums)
            # If compilation succeeds, try to execute the code which should fail
            if hasattr(module, "evil_code"):
                module.evil_code()
        error_msg = str(exc_info.value)
        assert any(
            msg in error_msg.lower() for msg in ["name 'open' is not defined", "open not found"]
        )


def test_custom_code_execution_not_permitted():
    """Test that code execution is blocked when environment variable is not set."""
    code = "def test(): return 42"
    checksums = {"md5": "e26ee9f0888fd40cc4d2264d49057bef"}
    with patch.dict(os.environ, {"AIRBYTE_ALLOW_CUSTOM_CODE": "false"}):
        with pytest.raises(AirbyteCustomCodeNotPermittedError):
            register_components_module_from_string(code, checksums)


def test_register_components_module_restricted_attributes():
    """Test that accessing restricted attributes is blocked."""
    code = """
class Evil:
    def __init__(self):
        self.__dict__ = {}
"""
    checksums = {"md5": "168fc66811e175f26a8cedb02aa723a4"}
    with patch.dict(os.environ, {"AIRBYTE_ALLOW_CUSTOM_CODE": "true"}):
        with pytest.raises(Exception) as exc_info:
            register_components_module_from_string(code, checksums)
        assert "__dict__" in str(exc_info.value)

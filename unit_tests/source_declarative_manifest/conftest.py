#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import hashlib
import os
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml


def hash_text(input_text: str, hash_type: Literal["md5", "sha256"] = "md5") -> str:
    hashers = {
        "md5": hashlib.md5,
        "sha256": hashlib.sha256,
    }
    hash_object = hashers[hash_type]()
    hash_object.update(input_text.encode())
    return hash_object.hexdigest()


def get_fixture_path(file_name) -> str:
    return os.path.join(os.path.dirname(__file__), file_name)


@pytest.fixture
def valid_remote_config():
    return get_fixture_path("resources/valid_remote_config.json")


@pytest.fixture
def invalid_remote_config():
    return get_fixture_path("resources/invalid_remote_config.json")


@pytest.fixture
def valid_local_manifest():
    return get_fixture_path("resources/valid_local_manifest.yaml")


@pytest.fixture
def invalid_local_manifest():
    return get_fixture_path("resources/invalid_local_manifest.yaml")


@pytest.fixture
def valid_local_manifest_yaml(valid_local_manifest):
    with open(valid_local_manifest, "r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def invalid_local_manifest_yaml(invalid_local_manifest):
    with open(invalid_local_manifest, "r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def valid_local_config_file():
    return get_fixture_path("resources/valid_local_pokeapi_config.json")


@pytest.fixture
def invalid_local_config_file():
    return get_fixture_path("resources/invalid_local_pokeapi_config.json")


@pytest.fixture
def py_components_config_dict() -> dict[str, Any]:
    manifest_dict = yaml.safe_load(
        get_fixture_path("resources/valid_py_components.yaml"),
    )
    custom_py_code_path = get_fixture_path("resources/valid_py_components_code.py")
    custom_py_code = Path(custom_py_code_path).read_text()
    combined_config_dict = {
        "__injected_declarative_manifest": manifest_dict,
        "__injected_components_py": custom_py_code,
        "__injected_components_py_checksum": {
            "md5": hash_text(custom_py_code, "md5"),
            "sha256": hash_text(custom_py_code, "sha256"),
        },
    }
    return combined_config_dict

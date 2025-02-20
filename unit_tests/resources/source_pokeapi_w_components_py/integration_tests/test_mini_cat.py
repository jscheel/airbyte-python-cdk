# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
import os
from pathlib import Path

from typing_extensions import override

from airbyte_cdk.sources.declarative.concurrent_declarative_source import (
    ConcurrentDeclarativeSource,
)
from airbyte_cdk.test.declarative.test_suites import (
    ConnectorTestScenario,
    DeclarativeSourceTestSuite,
    generate_tests,
)

CONNECTOR_ROOT = Path(__file__).parent.parent


def get_resource_path(file_name: str) -> Path:
    """Get the path to a resource file."""
    return CONNECTOR_ROOT / file_name


def pytest_generate_tests(metafunc):
    generate_tests(metafunc)


class TestSuiteSourcePokeAPI(DeclarativeSourceTestSuite):
    """Test suite for the source_pokeapi_w_components source.

    This class inherits from SourceTestSuiteBase and implements all of the tests in the suite.

    As long as the class name starts with "Test", pytest will automatically discover and run the
    tests in this class.
    """

    working_dir = CONNECTOR_ROOT
    manifest_path = get_resource_path("manifest.yaml")
    components_py_path = get_resource_path("components.py")
    acceptance_test_config_path = get_resource_path("acceptance-test-config.yml")

    # @override
    # @classmethod
    # def get_scenarios(cls) -> list[ConnectorTestScenario]:
    #     """Define the scenarios for the test suite."""
    #     return [
    #         ConnectorTestScenario(
    #             id="pikachu",
    #             config_dict={"pokemon_name": "pikachu"},
    #         ),
    #         ConnectorTestScenario(
    #             id="bulbasaur",
    #             config_dict={"pokemon_name": "bulbasaur"},
    #         ),
    #         ConnectorTestScenario(
    #             id="charmander",
    #             config_dict={"pokemon_name": "charmander"},
    #         ),
    #     ]

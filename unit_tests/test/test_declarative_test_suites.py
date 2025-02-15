# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Test declarative tests framework."""

from airbyte_cdk.test.declarative import (
    ConnectorTestSuiteBase,
    DestinationTestSuiteBase,
    SourceTestSuiteBase,
)


def test_declarative_test_suites():
    """Test declarative tests framework."""
    assert ConnectorTestSuiteBase()
    assert DestinationTestSuiteBase()
    assert SourceTestSuiteBase()

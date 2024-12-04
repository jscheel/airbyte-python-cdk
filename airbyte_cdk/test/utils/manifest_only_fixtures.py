# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

import pytest


@pytest.fixture(scope="session")
def connector_dir(request: pytest.FixtureRequest) -> Path:
    """Return the connector's root directory, which should always be the unit_tests folder's parent directory."""
    return Path(request.config.rootpath).parent


@pytest.fixture(scope="session")
def components_module(connector_dir: Path) -> Optional[ModuleType]:
    """Load and return the components module from the connector directory."""
    components_path = connector_dir / "components.py"
    if not components_path.exists():
        return None

    spec = importlib.util.spec_from_file_location("components", components_path)
    if spec is None:
        return None

    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        return None

    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def manifest_path(connector_dir: Path) -> Path:
    """Return the path to the connector's manifest file."""
    return connector_dir / "manifest.yaml"

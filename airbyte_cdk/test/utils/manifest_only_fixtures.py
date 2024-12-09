# Copyright (c) 2024 Airbyte, Inc., all rights reserved.


import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Optional

import pytest

# The following fixtures are used to load a manifest-only connector's components module and manifest file.
# They can be accessed from any test file in the connector's unit_tests directory by importing them as follows:

# from airbyte_cdk.test.utils.manifest_only_fixtures import components_module, connector_dir, manifest_path

# individual components can then be referenced as: components_module.<CustomComponentClass>


from typing import Optional
import pytest
from pathlib import Path
import importlib.util
from types import ModuleType


@pytest.fixture(scope="session")
def connector_dir(request: pytest.FixtureRequest) -> Path:
    """Return the connector's root directory."""
    
    path = Path(request.config.invocation_params.dir)
    # If the test is run locally from the connector's unit_tests directory, return the parent (connector) directory
    if path.name == "unit_tests":
        return path.parent
    # If the test is run in CI, return the current (connector) directory
    return path


@pytest.fixture(scope="session")
def components_module(connector_dir: Path) -> Optional[ModuleType]:
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

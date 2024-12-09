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


import os
from typing import Any, Optional
import pytest
from pathlib import Path
import importlib.util
from types import ModuleType


@pytest.fixture(scope="session")
def connector_dir(request: pytest.FixtureRequest) -> Path:
    """Return the connector's root directory."""
    print("\n=== CDK Path Resolution Debug ===")
    print(f"Config root path: {request.config.rootpath}")
    print(f"Invocation dir: {request.config.invocation_params.dir}")
    print(f"Current working dir: {os.getcwd()}")
    print(f"Test file dir: {getattr(request.module, '__file__', 'No file attribute')}")
    print(f"Environment variables: {dict(os.environ)}")
    print(f"Directory contents: {os.listdir(os.getcwd())}")
    print("==============================\n")
    
    path = Path(request.config.invocation_params.dir)
    resolved_path = path.parent
    print(f"Resolved connector dir: {resolved_path}")
    print(f"Resolved dir contents: {os.listdir(resolved_path) if resolved_path.exists() else 'Directory not found'}")
    
    return resolved_path


@pytest.fixture(scope="session")
def components_module(connector_dir: Path) -> Optional[ModuleType]:
    print("\n=== Components Module Debug ===")
    components_path = connector_dir / "components.py"
    print(f"Looking for components.py at: {components_path}")
    print(f"File exists: {components_path.exists()}")
    
    if not components_path.exists():
        print("components.py not found")
        return None
        
    spec = importlib.util.spec_from_file_location("components", components_path)
    print(f"Import spec created: {spec is not None}")
    
    if spec is None:
        return None
        
    module = importlib.util.module_from_spec(spec)
    print(f"Module created: {module is not None}")
    
    if spec.loader is None:
        return None
        
    spec.loader.exec_module(module)
    print("Module loaded successfully")
    print("===========================\n")
    
    return module


@pytest.fixture(scope="session")
def manifest_path(connector_dir: Path) -> Path:
    """Return the path to the connector's manifest file."""
    return connector_dir / "manifest.yaml"

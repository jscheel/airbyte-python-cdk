#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import re
from typing import Any

from referencing import Registry, Resource


def resolve_refs(schema: dict[str, Any]) -> dict[str, Any]:
    """
    For spec schemas generated using Pydantic models, the resulting JSON schema can contain refs between object
    relationships.
    """
    resource = Resource.from_contents(schema)
    registry = resource @ Registry()
    resolver = registry.resolver()

    str_schema = json.dumps(schema)
    for ref_block in re.findall(r'{"\$ref": "#\/definitions\/.+?(?="})"}', str_schema):
        ref = json.loads(ref_block)["$ref"]
        str_schema = str_schema.replace(ref_block, json.dumps(resolver.lookup(ref).contents))
    pyschema: dict[str, Any] = json.loads(str_schema)
    if "definitions" in pyschema:
        del pyschema["definitions"]
    return pyschema

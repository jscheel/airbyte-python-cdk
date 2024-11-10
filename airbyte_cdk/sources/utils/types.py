#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from typing import Union


JsonType = Union[dict[str, "JsonType"], list["JsonType"], str, int, float, bool, None]

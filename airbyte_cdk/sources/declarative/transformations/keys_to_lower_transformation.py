#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airbyte_cdk.sources.declarative.transformations import RecordTransformation


if TYPE_CHECKING:
    from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class KeysToLowerTransformation(RecordTransformation):
    def transform(
        self,
        record: dict[str, Any],
        config: Config | None = None,
        stream_state: StreamState | None = None,
        stream_slice: StreamSlice | None = None,
    ) -> None:
        for key in set(record.keys()):
            record[key.lower()] = record.pop(key)

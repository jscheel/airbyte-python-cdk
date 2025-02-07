#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from typing_extensions import override

from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class KeysToLowerTransformation(RecordTransformation):
    @override
    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        stream_interval: Optional[Dict[str, Any]] = None,
    ) -> None:
        for key in set(record.keys()):
            record[key.lower()] = record.pop(key)

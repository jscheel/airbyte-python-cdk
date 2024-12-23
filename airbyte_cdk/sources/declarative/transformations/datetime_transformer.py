#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Dict, List, Mapping, Optional

import dpath
import dpath.exceptions

from airbyte_cdk.sources.declarative.datetime.datetime_parser import DatetimeParser
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.types import Config, FieldPointer, StreamSlice, StreamState


@dataclass
class DateTimeNormalizer(RecordTransformation):
    """
    A transformation which transform specified datetime fields into RFC3339 format.

    Usage syntax:

    ```yaml
        my_stream:
            <other parameters..>
            transformations:
                - type: DateTimeNormalizer
                  field_pointers:
                    - ["path", "to", "field1"]
                    - ["path2"]
    ```

    Attributes:
        field_pointers (List[FieldPointer]): pointers to the fields that should be removed
    """

    field_pointers: List[FieldPointer]
    parameters: InitVar[Mapping[str, Any]]
    datetime_format: str = ""
    _parser = DatetimeParser()

    def transform(
        self,
        record: Dict[str, Any],
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> None:
        """
        :param record: The record to be transformed
        :return: the input record with the requested fields removed
        """
        for pointer in self.field_pointers:
            try:
                current_date_time_value: str | int = dpath.get(record, pointer)
                new_val = self._parser.parse(
                    date=current_date_time_value, format=self.datetime_format
                ).isoformat()
                dpath.set(record, pointer, new_val)
            except dpath.exceptions.PathNotFound:
                # if the (potentially nested) property does not exist, silently skip
                pass

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping

import pytest

from airbyte_cdk.sources.types import FieldPointer
from airbyte_cdk.sources.declarative.transformations.datetime_normalizer import DateTimeNormalizer


@pytest.mark.parametrize(
    ["input_record", "field_pointers", "datetime_format", "expected"],
    [
        [
            {"created_at": "2024-05-24T00:00", "k2": "v"},
            [["created_at"]],
            "",
            {"created_at": "2024-05-24T00:00:00", "k2": "v"},
        ],
        [
            {"created_at": "2024-05-24T00", "k2": "v"},
            [["created_at"]],
            "%Y-%m-%dT%H",
            {"created_at": "2024-05-24T00:00:00", "k2": "v"},
        ],
        [
            {"created_at": "2024-05-24 01:34:18 PDT", "k2": "v"},
            [["created_at"]],
            "",
            {"created_at": "2024-05-24T01:34:18-07:00", "k2": "v"},
        ],
        [
            {"date_field": "Jan 1, 2024", "k2": "v"},
            [["date_field"]],
            "%b %d, %Y",
            {"date_field": "2024-01-01", "k2": "v"},
        ],
    ],
    ids=[
        "datetime_hour_minutes",
        "datetime_hours",
        "datetime_custom_timezone",
        "date_format_in_words" # TODO: schema is needed here to treat field as date, not datetime
    ]
)
def test_date_time_normalizer_fields(
        input_record: Mapping[str, Any],
        field_pointers: List[FieldPointer],
        datetime_format: str,
        expected: Mapping[str, Any],
):
    DateTimeNormalizer(field_pointers=field_pointers, datetime_format=datetime_format, parameters={}).transform(input_record)
    assert input_record == expected

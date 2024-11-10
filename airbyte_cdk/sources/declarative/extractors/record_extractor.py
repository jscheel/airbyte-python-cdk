#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class RecordExtractor:
    """Responsible for translating an HTTP response into a list of records by extracting records from the response."""

    @abstractmethod
    def extract_records(
        self,
        response: requests.Response,
    ) -> Iterable[Mapping[str, Any]]:
        """Selects records from the response
        :param response: The response to extract the records from
        :return: List of Records extracted from the response
        """
        pass

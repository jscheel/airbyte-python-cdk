#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests

from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.types import Record


@dataclass
class Paginator(ABC, RequestOptionsProvider):
    """Defines the token to use to fetch the next page of records from the API.

    If needed, the Paginator will set request options to be set on the HTTP request to fetch the next page of records.
    If the next_page_token is the path to the next page of records, then it should be accessed through the `path` method
    """

    @abstractmethod
    def reset(self, reset_value: Any | None = None) -> None:
        """Reset the pagination's inner state"""

    @abstractmethod
    def next_page_token(
        self, response: requests.Response, last_page_size: int, last_record: Record | None
    ) -> Mapping[str, Any] | None:
        """Returns the next_page_token to use to fetch the next page of records.

        :param response: the response to process
        :param last_page_size: the number of records read from the response
        :param last_record: the last record extracted from the response
        :return: A mapping {"next_page_token": <token>} for the next page from the input response object. Returning None means there are no more pages to read in this response.
        """
        pass

    @abstractmethod
    def path(self) -> str | None:
        """Returns the URL path to hit to fetch the next page of records

        e.g: if you wanted to hit https://myapi.com/v1/some_entity then this will return "some_entity"

        :return: path to hit to fetch the next request. Returning None means the path is not defined by the next_page_token
        """
        pass

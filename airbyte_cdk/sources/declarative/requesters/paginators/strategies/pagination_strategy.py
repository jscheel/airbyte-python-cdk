#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

import requests

from airbyte_cdk.sources.types import Record


@dataclass
class PaginationStrategy:
    """
    Defines how to get the next page token
    """

    @property
    @abstractmethod
    def initial_token(self) -> Any | None:  # noqa: ANN401
        """
        Return the initial value of the token
        """

    @abstractmethod
    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Record | None,
        last_page_token_value: Any | None,  # noqa: ANN401
    ) -> Any | None:  # noqa: ANN401
        """
        :param response: response to process
        :param last_page_size: the number of records read from the response
        :param last_record: the last record extracted from the response
        :param last_page_token_value: The current value of the page token made on the last request
        :return: next page token. Returns None if there are no more pages to fetch
        """
        pass

    @abstractmethod
    def get_page_size(self) -> int | None:
        """
        :return: page size: The number of records to fetch in a page. Returns None if unspecified
        """

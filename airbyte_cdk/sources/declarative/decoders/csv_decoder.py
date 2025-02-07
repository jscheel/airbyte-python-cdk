#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import io
import logging
from dataclasses import InitVar, dataclass
from typing import Any, Generator, Mapping, MutableMapping

import pandas as pd
import requests

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder

logger = logging.getLogger("airbyte")


@dataclass
class CsvDecoder(Decoder):
    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.delimiter = parameters.get("delimiter", ",")
        self.encoding = parameters.get("encoding", "utf-8")
        self.chunk_size = 100

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        try:
            if not response.text.strip():
                yield {}
                return

            # First validate CSV structure
            lines = response.text.strip().split('\n')
            if not lines:
                yield {}
                return

            # Check if all rows have the same number of columns
            first_row_cols = len(lines[0].split(self.delimiter))
            if any(len(line.split(self.delimiter)) != first_row_cols for line in lines[1:]):
                yield {}
                return

            csv_data = io.StringIO(response.text)
            try:
                chunks = pd.read_csv(
                    csv_data,
                    chunksize=self.chunk_size,
                    iterator=True,
                    dtype=object,
                    delimiter=self.delimiter,
                    encoding=self.encoding
                )
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                yield {}
                return
            for chunk in chunks:
                for record in chunk.replace({pd.NA: None}).to_dict(orient="records"):
                    yield record
        except Exception as exc:
            logger.warning(
                f"Response cannot be parsed as CSV: {response.status_code=}, {response.text=}, {exc=}"
            )
            yield {}

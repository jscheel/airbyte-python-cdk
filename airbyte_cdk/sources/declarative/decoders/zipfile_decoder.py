#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import gzip
import io
import logging
import zipfile
from dataclasses import InitVar, dataclass
from typing import Any, Generator, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.declarative.decoders import Decoder
from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import Parser

logger = logging.getLogger("airbyte")


@dataclass
class ZipfileDecoder(Decoder):
    parser: Parser

    def is_stream_response(self) -> bool:
        return True

    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        try:
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile as e:
            logger.exception(e)
            logger.error(
                f"Received an invalid zip file in response to URL: {response.request.url}. "
                f"The size of the response body is: {len(response.content)}"
            )
            yield {}

        for filename in zip_file.namelist():
            with zip_file.open(filename) as file:
                buffered_file = io.BufferedReader(file)
                yield from self.parser.parse(buffered_file)

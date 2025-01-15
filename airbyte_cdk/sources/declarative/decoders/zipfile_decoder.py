#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import zipfile
from dataclasses import InitVar, dataclass
from io import BufferedReader, BytesIO
from typing import Any, Generator, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.decoders import Decoder
from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import Parser
from airbyte_cdk.utils import AirbyteTracedException

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
            zip_file = zipfile.ZipFile(BytesIO(response.content))
        except zipfile.BadZipFile as e:
            logger.error(
                f"Received an invalid zip file in response to URL: {response.request.url}. "
                f"The size of the response body is: {len(response.content)}"
            )
            raise AirbyteTracedException(
                message="Received an invalid zip file in response.",
                internal_message=f"Received an invalid zip file in response to URL: {response.request.url}.",
                failure_type=FailureType.system_error,
            ) from e

        for filename in zip_file.namelist():
            with zip_file.open(filename) as file:
                yield from self.parser.parse(BytesIO(file.read()))
                zip_file.close()

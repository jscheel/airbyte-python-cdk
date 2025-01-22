# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import logging
from collections.abc import Generator, Mapping
from typing import Any

import requests

from airbyte_cdk.sources.declarative.decoders.decoder import Decoder


logger = logging.getLogger("airbyte")


class NoopDecoder(Decoder):
    def is_stream_response(self) -> bool:
        return False

    def decode(  # type: ignore[override]  # Signature doesn't match base class
        self,
        response: requests.Response,  # noqa: ARG002
    ) -> Generator[Mapping[str, Any], None, None]:
        yield from [{}]

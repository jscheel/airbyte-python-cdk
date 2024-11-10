#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import abstractmethod
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any

import dpath
import pendulum
from pendulum import DateTime

from airbyte_cdk.sources.declarative.decoders.json_decoder import JsonDecoder
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.http_logger import format_http_message
from airbyte_cdk.sources.message import MessageRepository, NoopMessageRepository


if TYPE_CHECKING:
    import datetime
    from collections.abc import Mapping

    from isodate import Duration

    from airbyte_cdk.sources.declarative.decoders.decoder import Decoder
    from airbyte_cdk.sources.declarative.requesters.requester import Requester
    from airbyte_cdk.sources.types import Config


class TokenProvider:
    @abstractmethod
    def get_token(self) -> str:
        pass


@dataclass
class SessionTokenProvider(TokenProvider):
    login_requester: Requester
    session_token_path: list[str]
    expiration_duration: datetime.timedelta | Duration | None
    parameters: InitVar[Mapping[str, Any]]
    message_repository: MessageRepository = NoopMessageRepository()
    decoder: Decoder = field(default_factory=lambda: JsonDecoder(parameters={}))

    _next_expiration_time: DateTime | None = None
    _token: str | None = None

    def get_token(self) -> str:
        self._refresh_if_necessary()
        if self._token is None:
            raise ReadException("Failed to get session token, token is None")
        return self._token

    def _refresh_if_necessary(self) -> None:
        if self._next_expiration_time is None or self._next_expiration_time < pendulum.now():
            self._refresh()

    def _refresh(self) -> None:
        response = self.login_requester.send_request(
            log_formatter=lambda response: format_http_message(
                response,
                "Login request",
                "Obtains session token",
                None,
                is_auxiliary=True,
            ),
        )
        if response is None:
            raise ReadException("Failed to get session token, response got ignored by requester")
        session_token = dpath.get(next(self.decoder.decode(response)), self.session_token_path)
        if self.expiration_duration is not None:
            self._next_expiration_time = pendulum.now() + self.expiration_duration
        self._token = session_token  # type: ignore # Returned decoded response will be Mapping and therefore session_token will be str or None


@dataclass
class InterpolatedStringTokenProvider(TokenProvider):
    config: Config
    api_token: InterpolatedString | str
    parameters: Mapping[str, Any]

    def __post_init__(self) -> None:
        self._token = InterpolatedString.create(self.api_token, parameters=self.parameters)

    def get_token(self) -> str:
        return str(self._token.eval(self.config))

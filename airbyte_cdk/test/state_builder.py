# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from typing import Any, List

from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
)


class StateBuilder:
    def __init__(self) -> None:
        self._state: List[AirbyteStateMessage] = []

    def with_stream_state(self, stream_name: str, state: Any) -> "StateBuilder":
        self._state.append(
            AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_state=state
                    if isinstance(state, AirbyteStateBlob)
                    else AirbyteStateBlob(state),
                    stream_descriptor=StreamDescriptor(**{"name": stream_name}),
                ),
            )
        )
        return self

    def build(self) -> List[AirbyteStateMessage]:
        return self._state

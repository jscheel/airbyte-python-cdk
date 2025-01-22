#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from collections.abc import Iterable, Iterator, Mapping
from copy import deepcopy
from json import JSONDecodeError
from typing import Any

from airbyte_cdk.connector_builder.models import (
    AuxiliaryRequest,
    HttpRequest,
    HttpResponse,
    LogMessage,
    StreamRead,
    StreamReadPages,
    StreamReadSlices,
)
from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.models import (
    AirbyteControlMessage,
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    OrchestratorType,
    TraceType,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.declarative_source import DeclarativeSource
from airbyte_cdk.sources.utils.slice_logger import SliceLogger
from airbyte_cdk.sources.utils.types import JsonType
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.datetime_format_inferrer import DatetimeFormatInferrer
from airbyte_cdk.utils.schema_inferrer import SchemaInferrer, SchemaValidationException


class MessageGrouper:
    logger = logging.getLogger("airbyte.connector-builder")

    def __init__(self, max_pages_per_slice: int, max_slices: int, max_record_limit: int = 1000):  # noqa: ANN204
        self._max_pages_per_slice = max_pages_per_slice
        self._max_slices = max_slices
        self._max_record_limit = max_record_limit

    def _pk_to_nested_and_composite_field(
        self, field: str | list[str] | list[list[str]] | None
    ) -> list[list[str]]:
        if not field:
            return [[]]

        if isinstance(field, str):
            return [[field]]

        is_composite_key = isinstance(field[0], str)
        if is_composite_key:
            return [[i] for i in field]  # type: ignore  # the type of field is expected to be List[str] here

        return field  # type: ignore  # the type of field is expected to be List[List[str]] here

    def _cursor_field_to_nested_and_composite_field(
        self, field: str | list[str]
    ) -> list[list[str]]:
        if not field:
            return [[]]

        if isinstance(field, str):
            return [[field]]

        is_nested_key = isinstance(field[0], str)
        if is_nested_key:
            return [field]

        raise ValueError(f"Unknown type for cursor field `{field}")

    def get_message_groups(
        self,
        source: DeclarativeSource,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage],
        record_limit: int | None = None,
    ) -> StreamRead:
        if record_limit is not None and not (1 <= record_limit <= self._max_record_limit):
            raise ValueError(
                f"Record limit must be between 1 and {self._max_record_limit}. Got {record_limit}"
            )
        stream = source.streams(config)[
            0
        ]  # The connector builder currently only supports reading from a single stream at a time
        schema_inferrer = SchemaInferrer(
            self._pk_to_nested_and_composite_field(stream.primary_key),
            self._cursor_field_to_nested_and_composite_field(stream.cursor_field),
        )
        datetime_format_inferrer = DatetimeFormatInferrer()

        if record_limit is None:
            record_limit = self._max_record_limit
        else:
            record_limit = min(record_limit, self._max_record_limit)

        slices = []
        log_messages = []
        latest_config_update: AirbyteControlMessage = None
        auxiliary_requests = []
        for message_group in self._get_message_groups(
            self._read_stream(source, config, configured_catalog, state),
            schema_inferrer,
            datetime_format_inferrer,
            record_limit,
        ):
            if isinstance(message_group, AirbyteLogMessage):
                log_messages.append(
                    LogMessage(
                        message=message_group.message, level=message_group.level.value
                    )
                )
            elif isinstance(message_group, AirbyteTraceMessage):
                if message_group.type == TraceType.ERROR:
                    log_messages.append(
                        LogMessage(
                            message=message_group.error.message, level="ERROR", internal_message=message_group.error.internal_message, stacktrace=message_group.error.stack_trace
                        )
                    )
            elif isinstance(message_group, AirbyteControlMessage):
                if (
                    not latest_config_update
                    or latest_config_update.emitted_at <= message_group.emitted_at
                ):
                    latest_config_update = message_group
            elif isinstance(message_group, AuxiliaryRequest):
                auxiliary_requests.append(message_group)
            elif isinstance(message_group, StreamReadSlices):
                slices.append(message_group)
            else:
                raise ValueError(f"Unknown message group type: {type(message_group)}")  # noqa: TRY004

        try:
            # The connector builder currently only supports reading from a single stream at a time
            configured_stream = configured_catalog.streams[0]
            schema = schema_inferrer.get_stream_schema(configured_stream.stream.name)
        except SchemaValidationException as exception:
            for validation_error in exception.validation_errors:
                log_messages.append(LogMessage(validation_error, "ERROR"))  # noqa: PERF401
            schema = exception.schema

        return StreamRead(
            logs=log_messages,
            slices=slices,
            test_read_limit_reached=self._has_reached_limit(slices),
            auxiliary_requests=auxiliary_requests,
            inferred_schema=schema,
            latest_config_update=self._clean_config(latest_config_update.connectorConfig.config)
            if latest_config_update
            else None,
            inferred_datetime_formats=datetime_format_inferrer.get_inferred_datetime_formats(),
        )

    def _get_message_groups(  # noqa: PLR0912, PLR0915
        self,
        messages: Iterator[AirbyteMessage],
        schema_inferrer: SchemaInferrer,
        datetime_format_inferrer: DatetimeFormatInferrer,
        limit: int,
    ) -> Iterable[
        StreamReadPages | AirbyteControlMessage | AirbyteLogMessage | AirbyteTraceMessage | AuxiliaryRequest
    ]:
        """
        Message groups are partitioned according to when request log messages are received. Subsequent response log messages
        and record messages belong to the prior request log message and when we encounter another request, append the latest
        message group, until <limit> records have been read.

        Messages received from the CDK read operation will always arrive in the following order:
        {type: LOG, log: {message: "request: ..."}}
        {type: LOG, log: {message: "response: ..."}}
        ... 0 or more record messages
        {type: RECORD, record: {data: ...}}
        {type: RECORD, record: {data: ...}}
        Repeats for each request/response made

        Note: The exception is that normal log messages can be received at any time which are not incorporated into grouping
        """
        records_count = 0
        at_least_one_page_in_group = False
        current_page_records: list[Mapping[str, Any]] = []
        current_slice_descriptor: dict[str, Any] | None = None
        current_slice_pages: list[StreamReadPages] = []
        current_page_request: HttpRequest | None = None
        current_page_response: HttpResponse | None = None
        latest_state_message: dict[str, Any] | None = None

        while records_count < limit and (message := next(messages, None)):
            json_object = self._parse_json(message.log) if message.type == MessageType.LOG else None
            if json_object is not None and not isinstance(json_object, dict):
                raise ValueError(
                    f"Expected log message to be a dict, got {json_object} of type {type(json_object)}"
                )
            json_message: dict[str, JsonType] | None = json_object
            if self._need_to_close_page(at_least_one_page_in_group, message, json_message):
                self._close_page(
                    current_page_request,
                    current_page_response,
                    current_slice_pages,
                    current_page_records,
                )
                current_page_request = None
                current_page_response = None

            if (
                at_least_one_page_in_group
                and message.type == MessageType.LOG
                and message.log.message.startswith(SliceLogger.SLICE_LOG_PREFIX)  # type: ignore[union-attr] # AirbyteMessage with MessageType.LOG has log.message
            ):
                yield StreamReadSlices(
                    pages=current_slice_pages,
                    slice_descriptor=current_slice_descriptor,
                    state=[latest_state_message] if latest_state_message else [],
                )
                current_slice_descriptor = self._parse_slice_description(message.log.message)  # type: ignore[union-attr] # AirbyteMessage with MessageType.LOG has log.message
                current_slice_pages = []
                at_least_one_page_in_group = False
            elif message.type == MessageType.LOG and message.log.message.startswith(  # type: ignore[union-attr] # None doesn't have 'message'
                SliceLogger.SLICE_LOG_PREFIX
            ):
                # parsing the first slice
                current_slice_descriptor = self._parse_slice_description(message.log.message)  # type: ignore[union-attr] # AirbyteMessage with MessageType.LOG has log.message
            elif message.type == MessageType.LOG:
                if json_message is not None and self._is_http_log(json_message):
                    if self._is_auxiliary_http_request(json_message):
                        airbyte_cdk = json_message.get("airbyte_cdk", {})
                        if not isinstance(airbyte_cdk, dict):
                            raise ValueError(
                                f"Expected airbyte_cdk to be a dict, got {airbyte_cdk} of type {type(airbyte_cdk)}"
                            )
                        stream = airbyte_cdk.get("stream", {})
                        if not isinstance(stream, dict):
                            raise ValueError(
                                f"Expected stream to be a dict, got {stream} of type {type(stream)}"
                            )
                        title_prefix = (
                            "Parent stream: " if stream.get("is_substream", False) else ""
                        )
                        http = json_message.get("http", {})
                        if not isinstance(http, dict):
                            raise ValueError(
                                f"Expected http to be a dict, got {http} of type {type(http)}"
                            )
                        yield AuxiliaryRequest(
                            title=title_prefix + str(http.get("title", None)),
                            description=str(http.get("description", None)),
                            request=self._create_request_from_log_message(json_message),
                            response=self._create_response_from_log_message(json_message),
                        )
                    else:
                        at_least_one_page_in_group = True
                        current_page_request = self._create_request_from_log_message(json_message)
                        current_page_response = self._create_response_from_log_message(json_message)
                else:
                    yield message.log
            elif message.type == MessageType.TRACE:
                if message.trace.type == TraceType.ERROR:  # type: ignore[union-attr] # AirbyteMessage with MessageType.TRACE has trace.type
                    yield message.trace
            elif message.type == MessageType.RECORD:
                current_page_records.append(message.record.data)  # type: ignore[arg-type, union-attr] # AirbyteMessage with MessageType.RECORD has record.data
                records_count += 1
                schema_inferrer.accumulate(message.record)
                datetime_format_inferrer.accumulate(message.record)
            elif (
                message.type == MessageType.CONTROL
                and message.control.type == OrchestratorType.CONNECTOR_CONFIG  # type: ignore[union-attr] # None doesn't have 'type'
            ):
                yield message.control
            elif message.type == MessageType.STATE:
                latest_state_message = message.state  # type: ignore[assignment]
        if current_page_request or current_page_response or current_page_records:
            self._close_page(
                current_page_request,
                current_page_response,
                current_slice_pages,
                current_page_records,
            )
            yield StreamReadSlices(
                pages=current_slice_pages,
                slice_descriptor=current_slice_descriptor,
                state=[latest_state_message] if latest_state_message else [],
            )

    @staticmethod
    def _need_to_close_page(
        at_least_one_page_in_group: bool,  # noqa: FBT001
        message: AirbyteMessage,
        json_message: dict[str, Any] | None,
    ) -> bool:
        return (
            at_least_one_page_in_group
            and message.type == MessageType.LOG
            and (
                MessageGrouper._is_page_http_request(json_message)
                or message.log.message.startswith("slice:")  # type: ignore[union-attr] # AirbyteMessage with MessageType.LOG has log.message
            )
        )

    @staticmethod
    def _is_page_http_request(json_message: dict[str, Any] | None) -> bool:
        if not json_message:
            return False
        return MessageGrouper._is_http_log(
            json_message
        ) and not MessageGrouper._is_auxiliary_http_request(json_message)

    @staticmethod
    def _is_http_log(message: dict[str, JsonType]) -> bool:
        return bool(message.get("http", False))

    @staticmethod
    def _is_auxiliary_http_request(message: dict[str, Any] | None) -> bool:
        """
        A auxiliary request is a request that is performed and will not directly lead to record for the specific stream it is being queried.
        A couple of examples are:
        * OAuth authentication
        * Substream slice generation
        """
        if not message:
            return False

        is_http = MessageGrouper._is_http_log(message)
        return is_http and message.get("http", {}).get("is_auxiliary", False)

    @staticmethod
    def _close_page(
        current_page_request: HttpRequest | None,
        current_page_response: HttpResponse | None,
        current_slice_pages: list[StreamReadPages],
        current_page_records: list[Mapping[str, Any]],
    ) -> None:
        """
        Close a page when parsing message groups
        """
        current_slice_pages.append(
            StreamReadPages(
                request=current_page_request,
                response=current_page_response,
                records=deepcopy(current_page_records),  # type: ignore [arg-type]
            )
        )
        current_page_records.clear()

    def _read_stream(
        self,
        source: DeclarativeSource,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage],
    ) -> Iterator[AirbyteMessage]:
        # the generator can raise an exception
        # iterate over the generated messages. if next raise an exception, catch it and yield it as an AirbyteLogMessage
        try:
            yield from AirbyteEntrypoint(source).read(
                source.spec(self.logger), config, configured_catalog, state
            )
        except AirbyteTracedException as traced_exception:
            # Look for this message which indicates that it is the "final exception" raised by AbstractSource.
            # If it matches, don't yield this as we don't need to show this in the Builder.
            # This is somewhat brittle as it relies on the message string, but if they drift then the worst case
            # is that this message will be shown in the Builder.
            if (
                traced_exception.message is not None
                and "During the sync, the following streams did not sync successfully"
                in traced_exception.message
            ):
                return
            yield traced_exception.as_airbyte_message()
        except Exception as e:
            error_message = f"{e.args[0] if len(e.args) > 0 else str(e)}"
            yield AirbyteTracedException.from_exception(
                e, message=error_message
            ).as_airbyte_message()

    @staticmethod
    def _parse_json(log_message: AirbyteLogMessage) -> JsonType:
        # TODO: As a temporary stopgap, the CDK emits request/response data as a log message string. Ideally this should come in the
        # form of a custom message object defined in the Airbyte protocol, but this unblocks us in the immediate while the
        # protocol change is worked on.
        try:
            json_object: JsonType = json.loads(log_message.message)
            return json_object  # noqa: TRY300
        except JSONDecodeError:
            return None

    @staticmethod
    def _create_request_from_log_message(json_http_message: dict[str, Any]) -> HttpRequest:
        url = json_http_message.get("url", {}).get("full", "")
        request = json_http_message.get("http", {}).get("request", {})
        return HttpRequest(
            url=url,
            http_method=request.get("method", ""),
            headers=request.get("headers"),
            body=request.get("body", {}).get("content", ""),
        )

    @staticmethod
    def _create_response_from_log_message(json_http_message: dict[str, Any]) -> HttpResponse:
        response = json_http_message.get("http", {}).get("response", {})
        body = response.get("body", {}).get("content", "")
        return HttpResponse(
            status=response.get("status_code"), body=body, headers=response.get("headers")
        )

    def _has_reached_limit(self, slices: list[StreamReadSlices]) -> bool:
        if len(slices) >= self._max_slices:
            return True

        record_count = 0

        for _slice in slices:
            if len(_slice.pages) >= self._max_pages_per_slice:
                return True
            for page in _slice.pages:
                record_count += len(page.records)
                if record_count >= self._max_record_limit:
                    return True
        return False

    def _parse_slice_description(self, log_message: str) -> dict[str, Any]:
        return json.loads(log_message.replace(SliceLogger.SLICE_LOG_PREFIX, "", 1))  # type: ignore

    @staticmethod
    def _clean_config(config: dict[str, Any]) -> dict[str, Any]:
        cleaned_config = deepcopy(config)
        for key in config:
            if key.startswith("__"):
                del cleaned_config[key]
        return cleaned_config

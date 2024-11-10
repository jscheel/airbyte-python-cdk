#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from io import IOBase
from typing import Any

from unit_tests.sources.file_based.in_memory_files_source import InMemoryFilesStreamReader

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.discovery_policy import DefaultDiscoveryPolicy
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.csv_parser import CsvParser
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.file_types.jsonl_parser import JsonlParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy
from airbyte_cdk.sources.file_based.stream.concurrent.cursor import FileBasedConcurrentCursor
from airbyte_cdk.sources.file_based.stream.cursor import DefaultFileBasedCursor


class EmptySchemaParser(CsvParser):
    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> dict[str, Any]:
        return {}


class LowInferenceLimitDiscoveryPolicy(DefaultDiscoveryPolicy):
    def get_max_n_files_for_schema_inference(self, parser: FileTypeParser) -> int:
        return 1


class LowInferenceBytesJsonlParser(JsonlParser):
    MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE = 1


class TestErrorListMatchingFilesInMemoryFilesStreamReader(InMemoryFilesStreamReader):
    def get_matching_files(
        self,
        globs: list[str],
        from_date: datetime | None = None,
    ) -> list[RemoteFile]:
        raise Exception("Error listing files")


class TestErrorOpenFileInMemoryFilesStreamReader(InMemoryFilesStreamReader):
    def open_file(
        self,
        file: RemoteFile,
        file_read_mode: FileReadMode,
        encoding: str | None,
        logger: logging.Logger,
    ) -> IOBase:
        raise Exception("Error opening file")


class FailingSchemaValidationPolicy(AbstractSchemaValidationPolicy):
    ALWAYS_FAIL = "always_fail"
    validate_schema_before_sync = True

    def record_passes_validation_policy(
        self, record: Mapping[str, Any], schema: Mapping[str, Any] | None
    ) -> bool:
        return False


class LowHistoryLimitCursor(DefaultFileBasedCursor):
    DEFAULT_MAX_HISTORY_SIZE = 3


class LowHistoryLimitConcurrentCursor(FileBasedConcurrentCursor):
    DEFAULT_MAX_HISTORY_SIZE = 3


def make_remote_files(files: list[str]) -> list[RemoteFile]:
    return [
        RemoteFile(
            uri=f,
            last_modified=datetime.strptime("2023-06-05T03:54:07.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        for f in files
    ]

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

import csv
import io
import json
import logging
import tempfile
from collections.abc import Iterable, Mapping
from datetime import datetime
from io import IOBase
from typing import Any

import avro.io as ai
import avro.schema as avro_schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from avro import datafile
from pydantic.v1 import AnyUrl

from airbyte_cdk.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteCatalogSerializer
from airbyte_cdk.sources.file_based.availability_strategy import (
    AbstractFileBasedAvailabilityStrategy,
    DefaultFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec
from airbyte_cdk.sources.file_based.discovery_policy import (
    AbstractDiscoveryPolicy,
    DefaultDiscoveryPolicy,
)
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_validation_policies import (
    DEFAULT_SCHEMA_VALIDATION_POLICIES,
    AbstractSchemaValidationPolicy,
)
from airbyte_cdk.sources.file_based.stream.cursor import (
    AbstractFileBasedCursor,
    DefaultFileBasedCursor,
)
from airbyte_cdk.sources.source import TState


class InMemoryFilesSource(FileBasedSource):
    _concurrency_level = 10

    def __init__(
        self,
        files: Mapping[str, Any],
        file_type: str,
        availability_strategy: AbstractFileBasedAvailabilityStrategy | None,
        discovery_policy: AbstractDiscoveryPolicy | None,
        validation_policies: Mapping[str, AbstractSchemaValidationPolicy],
        parsers: Mapping[str, FileTypeParser],
        stream_reader: AbstractFileBasedStreamReader | None,
        catalog: Mapping[str, Any] | None,
        config: Mapping[str, Any] | None,
        state: TState | None,
        file_write_options: Mapping[str, Any],
        cursor_cls: AbstractFileBasedCursor | None,
    ):
        # Attributes required for test purposes
        self.files = files
        self.file_type = file_type
        self.catalog = catalog
        self.configured_catalog = (
            ConfiguredAirbyteCatalogSerializer.load(self.catalog) if self.catalog else None
        )
        self.config = config
        self.state = state

        # Source setup
        stream_reader = stream_reader or InMemoryFilesStreamReader(
            files=files, file_type=file_type, file_write_options=file_write_options
        )
        availability_strategy = availability_strategy or DefaultFileBasedAvailabilityStrategy(
            stream_reader
        )  # type: ignore[assignment]
        super().__init__(
            stream_reader,
            spec_class=InMemorySpec,
            catalog=self.configured_catalog,
            config=self.config,
            state=self.state,
            availability_strategy=availability_strategy,
            discovery_policy=discovery_policy or DefaultDiscoveryPolicy(),
            parsers=parsers,
            validation_policies=validation_policies or DEFAULT_SCHEMA_VALIDATION_POLICIES,
            cursor_cls=cursor_cls or DefaultFileBasedCursor,
        )

    def read_catalog(self, catalog_path: str) -> ConfiguredAirbyteCatalog:
        return self.configured_catalog


class InMemoryFilesStreamReader(AbstractFileBasedStreamReader):
    def __init__(
        self,
        files: Mapping[str, Mapping[str, Any]],
        file_type: str,
        file_write_options: Mapping[str, Any] | None = None,
    ):
        self.files = files
        self.file_type = file_type
        self.file_write_options = file_write_options
        super().__init__()

    @property
    def config(self) -> AbstractFileBasedSpec | None:
        return self._config

    @config.setter
    def config(self, value: AbstractFileBasedSpec) -> None:
        self._config = value

    def get_matching_files(
        self,
        globs: list[str],
        prefix: str | None,
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        yield from self.filter_files_by_globs_and_start_date(
            [
                RemoteFile(
                    uri=f,
                    mime_type=data.get("mime_type", None),
                    last_modified=datetime.strptime(data["last_modified"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                )
                for f, data in self.files.items()
            ],
            globs,
        )

    def file_size(self, file: RemoteFile) -> int:
        return 0

    def get_file(
        self, file: RemoteFile, local_directory: str, logger: logging.Logger
    ) -> dict[str, Any]:
        return {}

    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: str | None, logger: logging.Logger
    ) -> IOBase:
        if self.file_type == "csv":
            return self._make_csv_file_contents(file.uri)
        if self.file_type == "jsonl":
            return self._make_jsonl_file_contents(file.uri)
        if self.file_type == "unstructured":
            return self._make_binary_file_contents(file.uri)
        raise NotImplementedError(f"No implementation for file type: {self.file_type}")

    def _make_csv_file_contents(self, file_name: str) -> IOBase:
        # Some tests define the csv as an array of strings to make it easier to validate the handling
        # of quotes, delimiter, and escpare chars.
        if isinstance(self.files[file_name]["contents"][0], str):
            return io.StringIO("\n".join([s.strip() for s in self.files[file_name]["contents"]]))

        fh = io.StringIO()

        if self.file_write_options:
            csv.register_dialect("in_memory_dialect", **self.file_write_options)
            writer = csv.writer(fh, dialect="in_memory_dialect")
            writer.writerows(self.files[file_name]["contents"])
            csv.unregister_dialect("in_memory_dialect")
        else:
            writer = csv.writer(fh)
            writer.writerows(self.files[file_name]["contents"])
        fh.seek(0)
        return fh

    def _make_jsonl_file_contents(self, file_name: str) -> IOBase:
        fh = io.BytesIO()

        for line in self.files[file_name]["contents"]:
            try:
                fh.write((json.dumps(line) + "\n").encode("utf-8"))
            except TypeError:
                # Intentionally trigger json validation error
                fh.write((str(line) + "\n").encode("utf-8"))
        fh.seek(0)
        return fh

    def _make_binary_file_contents(self, file_name: str) -> IOBase:
        fh = io.BytesIO()

        fh.write(self.files[file_name]["contents"])
        fh.seek(0)
        return fh


class InMemorySpec(AbstractFileBasedSpec):
    @classmethod
    def documentation_url(cls) -> AnyUrl:
        return AnyUrl(
            scheme="https", url="https://docs.airbyte.com/integrations/sources/in_memory_files"
        )  # type: ignore


class TemporaryParquetFilesStreamReader(InMemoryFilesStreamReader):
    """A file reader that writes RemoteFiles to a temporary file and then reads them back."""

    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: str | None, logger: logging.Logger
    ) -> IOBase:
        return io.BytesIO(self._create_file(file.uri))

    def _create_file(self, file_name: str) -> bytes:
        contents = self.files[file_name]["contents"]
        schema = self.files[file_name].get("schema")

        df = pd.DataFrame(contents[1:], columns=contents[0])
        with tempfile.TemporaryFile() as fp:
            table = pa.Table.from_pandas(df, schema)
            pq.write_table(table, fp)

            fp.seek(0)
            return fp.read()


class TemporaryAvroFilesStreamReader(InMemoryFilesStreamReader):
    """A file reader that writes RemoteFiles to a temporary file and then reads them back."""

    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: str | None, logger: logging.Logger
    ) -> IOBase:
        return io.BytesIO(self._make_file_contents(file.uri))

    def _make_file_contents(self, file_name: str) -> bytes:
        contents = self.files[file_name]["contents"]
        schema = self.files[file_name]["schema"]
        stream_schema = avro_schema.make_avsc_object(schema)

        rec_writer = ai.DatumWriter(stream_schema)
        with tempfile.TemporaryFile() as fp:
            file_writer = datafile.DataFileWriter(fp, rec_writer, stream_schema)
            for content in contents:
                data = {col["name"]: content[i] for i, col in enumerate(schema["fields"])}
                file_writer.append(data)
            file_writer.flush()
            fp.seek(0)
            return fp.read()


class TemporaryExcelFilesStreamReader(InMemoryFilesStreamReader):
    """A file reader that writes RemoteFiles to a temporary file and then reads them back."""

    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: str | None, logger: logging.Logger
    ) -> IOBase:
        return io.BytesIO(self._make_file_contents(file.uri))

    def _make_file_contents(self, file_name: str) -> bytes:
        contents = self.files[file_name]["contents"]
        df = pd.DataFrame(contents)

        with io.BytesIO() as fp:
            writer = pd.ExcelWriter(fp, engine="xlsxwriter")
            df.to_excel(writer, index=False, sheet_name="Sheet1")
            writer._save()
            fp.seek(0)
            return fp.read()

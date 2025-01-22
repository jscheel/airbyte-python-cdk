from collections.abc import Mapping
from typing import Any, Type  # noqa: F401, UP035

from .avro_parser import AvroParser
from .csv_parser import CsvParser
from .excel_parser import ExcelParser
from .file_transfer import FileTransfer
from .file_type_parser import FileTypeParser
from .jsonl_parser import JsonlParser
from .parquet_parser import ParquetParser
from .unstructured_parser import UnstructuredParser
from airbyte_cdk.sources.file_based.config.avro_format import AvroFormat
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat
from airbyte_cdk.sources.file_based.config.excel_format import ExcelFormat
from airbyte_cdk.sources.file_based.config.jsonl_format import JsonlFormat
from airbyte_cdk.sources.file_based.config.parquet_format import ParquetFormat
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat


default_parsers: Mapping[type[Any], FileTypeParser] = {
    AvroFormat: AvroParser(),
    CsvFormat: CsvParser(),
    ExcelFormat: ExcelParser(),
    JsonlFormat: JsonlParser(),
    ParquetFormat: ParquetParser(),
    UnstructuredFormat: UnstructuredParser(),
}

__all__ = [
    "AvroParser",
    "CsvParser",
    "ExcelParser",
    "JsonlParser",
    "ParquetParser",
    "UnstructuredParser",
    "FileTransfer",
    "default_parsers",
]

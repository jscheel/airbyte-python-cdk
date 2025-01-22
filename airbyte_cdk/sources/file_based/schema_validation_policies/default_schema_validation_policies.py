#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
from typing import Any

from airbyte_cdk.sources.file_based.config.file_based_stream_config import ValidationPolicy
from airbyte_cdk.sources.file_based.exceptions import (
    FileBasedSourceError,
    StopSyncPerValidationPolicy,
)
from airbyte_cdk.sources.file_based.schema_helpers import conforms_to_schema
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy


class EmitRecordPolicy(AbstractSchemaValidationPolicy):
    name = "emit_record"

    def record_passes_validation_policy(
        self,
        record: Mapping[str, Any],
        schema: Mapping[str, Any] | None,  # noqa: ARG002
    ) -> bool:
        return True


class SkipRecordPolicy(AbstractSchemaValidationPolicy):
    name = "skip_record"

    def record_passes_validation_policy(
        self, record: Mapping[str, Any], schema: Mapping[str, Any] | None
    ) -> bool:
        return schema is not None and conforms_to_schema(record, schema)


class WaitForDiscoverPolicy(AbstractSchemaValidationPolicy):
    name = "wait_for_discover"
    validate_schema_before_sync = True

    def record_passes_validation_policy(
        self, record: Mapping[str, Any], schema: Mapping[str, Any] | None
    ) -> bool:
        if schema is None or not conforms_to_schema(record, schema):
            raise StopSyncPerValidationPolicy(
                FileBasedSourceError.STOP_SYNC_PER_SCHEMA_VALIDATION_POLICY
            )
        return True


DEFAULT_SCHEMA_VALIDATION_POLICIES = {
    ValidationPolicy.emit_record: EmitRecordPolicy(),
    ValidationPolicy.skip_record: SkipRecordPolicy(),
    ValidationPolicy.wait_for_discover: WaitForDiscoverPolicy(),
}

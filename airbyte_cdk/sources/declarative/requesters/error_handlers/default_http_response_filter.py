#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import requests

from airbyte_cdk.sources.declarative.requesters.error_handlers.http_response_filter import (
    HttpResponseFilter,
)
from airbyte_cdk.sources.streams.http.error_handlers.default_error_mapping import (
    DEFAULT_ERROR_MAPPING,
)
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    create_fallback_error_resolution,
)


class DefaultHttpResponseFilter(HttpResponseFilter):
    def matches(
        self, response_or_exception: requests.Response | Exception | None
    ) -> ErrorResolution | None:
        default_mapped_error_resolution = None

        if isinstance(response_or_exception, (requests.Response, Exception)):  # noqa: UP038
            mapped_key: int | type = (
                response_or_exception.status_code
                if isinstance(response_or_exception, requests.Response)
                else response_or_exception.__class__
            )

            default_mapped_error_resolution = DEFAULT_ERROR_MAPPING.get(mapped_key)

        return (
            default_mapped_error_resolution  # noqa: FURB110
            if default_mapped_error_resolution
            else create_fallback_error_resolution(response_or_exception)
        )

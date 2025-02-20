#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
from copy import deepcopy

import json
from unittest.mock import MagicMock
from airbyte_cdk.sources.declarative.concurrent_declarative_source import ConcurrentDeclarativeSource
from airbyte_cdk.sources.embedded.catalog import (
    to_configured_catalog,
    to_configured_stream,
)
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse

logger = logging.getLogger("test")

_CONFIG = {"start_date": "2024-07-01T00:00:00.000Z"}

_MANIFEST = {
            "version": "6.7.0",
            "streams": [
                {
                    "type": "DeclarativeStream",
                    "$parameters": {
                        "name": "items",
                        "primary_key": "id",
                        "url_base": "https://api.test.com",
                    },
                    "retriever": {
                        "paginator": {
                            "type": "DefaultPaginator",
                            "page_token_option": {
                                "type": "RequestOption",
                                "inject_into": "request_parameter",
                                "field_name": "page",
                            },
                            "pagination_strategy": {
                                "type": "PageIncrement",
                                "page_size": 1
                            },
                        },
                        "requester": {
                            "path": "/items",
                            "authenticator": {
                                "type": "BearerAuthenticator",
                                "api_token": "{{ config.apikey }}",
                            },
                            "api_budget": {
                                "type": "HttpApiBudget",
                                "policy": {
                                    "type": "MovingWindowCallRatePolicy",
                                    "matcher": {
                                            "type": "HttpRequestMatcher",
                                            "http_method": "GET",
                                            "url": "https://api.test.com/items"
                                    },
                                    "rate":
                                        {
                                            "type": "CallRateLimit",
                                            "limit": 2,
                                            "interval": "PT1S"
                                        }
                                }
                            }
                        },
                        "record_selector": {"extractor": {"field_path": ["result"]}},
                    },
                }
            ],
            "check": {"type": "CheckStream", "stream_names": ["lists"]},
        }


def mock_http_requests(http_mocker):
    """Helper function to mock HTTP requests and responses."""
    base_url = "https://api.test.com/items"

    http_mocker.get(
        HttpRequest(url=base_url),
        HttpResponse(body=json.dumps({"result": [{"id": 1, "name": "item_1"}, {"id": 2, "name": "item_2"}]})),
    )

    for page in range(1, 2):
        http_mocker.get(
            HttpRequest(url=f"{base_url}?page={page}"),
            HttpResponse(body=json.dumps({"result": [
                {"id": page + 1, "name": f"item_{page + 1}"},
                {"id": page + 2, "name": f"item_{page + 2}"}
            ]}))
        )

    http_mocker.get(
        HttpRequest(url=f"{base_url}?page=2"),
        HttpResponse(body=json.dumps({"result": []})),
    )


def test_declarative_api_budget(caplog):
    manifest = deepcopy(_MANIFEST)

    with HttpMocker() as http_mocker:
        mock_http_requests(http_mocker)

        source = ConcurrentDeclarativeSource(
            source_config=manifest, config=_CONFIG, catalog=None, state=None
        )

        actual_catalog = source.discover(logger=source.logger, config=_CONFIG)
        configured_streams = [
            to_configured_stream(stream, primary_key=stream.source_defined_primary_key)
            for stream in actual_catalog.streams
        ]
        configured_catalog = to_configured_catalog(configured_streams)

        with caplog.at_level(logging.INFO):
            list(source.read(MagicMock(), _CONFIG, configured_catalog))
            assert "reached call limit" in caplog.text
#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from datetime import datetime

from pydantic.v1 import BaseModel


class RemoteFile(BaseModel):
    """A file in a file-based stream."""

    uri: str
    last_modified: datetime
    mime_type: str | None = None

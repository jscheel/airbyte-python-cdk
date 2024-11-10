#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic.v1 import BaseModel


if TYPE_CHECKING:
    from datetime import datetime


class RemoteFile(BaseModel):
    """A file in a file-based stream."""

    uri: str
    last_modified: datetime
    mime_type: str | None = None

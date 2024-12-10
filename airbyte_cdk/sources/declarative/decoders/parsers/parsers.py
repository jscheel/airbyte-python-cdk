#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Generator, MutableMapping, Union


@dataclass
class Parser:
    """
    Parser strategy to convert byte data into a MutableMapping[str, Any].
    """

    @abstractmethod
    def parse(self, data: bytes) -> Generator[MutableMapping[str, Any], None, None]:
        pass


class JsonParser(Parser):
    def parse(self, data: Union[str, bytes, bytearray]) -> Generator[MutableMapping[str, Any], None, None]:
        yield json.loads(data)

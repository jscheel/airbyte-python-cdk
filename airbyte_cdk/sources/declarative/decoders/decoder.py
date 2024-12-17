#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Generator, MutableMapping

import requests


@dataclass
class Decoder:
    """
    Decoder strategy to transform a requests.Response into a Mapping[str, Any]
    """

    @abstractmethod
    def is_stream_response(self) -> bool:
        """
        Set to True if you'd like to use stream=True option in http requester
        """

    @abstractmethod
    def decode(
        self, response: requests.Response
    ) -> Generator[MutableMapping[str, Any], None, None]:
        """
        Decodes a requests.Response into a Mapping[str, Any] or an array
        :param response: the response to decode
        :return: Generator of Mapping describing the response
        """


class ParserSelector:
    def select(self, response) -> Parser:
        match response.header["Content-type"]:
            case "application/json":
                return response.json()
            case "ijasdifjdasif"


class Parser(ABC):
    def parser(self, bytes) -> Generator[MutableMapping[str, Any], None, None]:
        pass

class GenericParserDecoder(Decoder):

    def is_stream_response(self) -> bool:
        pass

    def decode(self, response: requests.Response) -> Generator[MutableMapping[str, Any], None, None]:
        parser = self._parser_selector.select(response)
        return parser.parse(response.content)


class GzipParser(Parser):
    def __init__(self, other_parser):
        self._other_parser =other_parser

    def parse(self, bytes) -> Generator[MutableMapping[str, Any], None, None]:
        decoded_response = # do the gzip part
        return other_parser.parse(decoded_response)


class ZipParser(Parser):
    def __init__(self, other_parser: Parser):
        self._other_parser = other_parser  # it can be GzipParser or JsonlParser

    def parse(self, bytes) -> Generator[MutableMapping[str, Any], None, None]:
        # do the zip part
        file_from_zip = get_the_file_content
        return other_parser.parse(file_from_zip)

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from airbyte_cdk.sources.streams.concurrent.partitions.record import Record


class Partition(ABC):
    """A partition is responsible for reading a specific set of data from a source."""

    @abstractmethod
    def read(self) -> Iterable[Record]:
        """Reads the data from the partition.
        :return: An iterable of records.
        """
        pass

    @abstractmethod
    def to_slice(self) -> Mapping[str, Any] | None:
        """Converts the partition to a slice that can be serialized and deserialized.

        Note: it would have been interesting to have a type of `Mapping[str, Comparable]` to simplify typing but some slices can have nested
         values ([example](https://github.com/airbytehq/airbyte/blob/1ce84d6396e446e1ac2377362446e3fb94509461/airbyte-integrations/connectors/source-stripe/source_stripe/streams.py#L584-L596))
        :return: A mapping representing a slice
        """
        pass

    @abstractmethod
    def stream_name(self) -> str:
        """Returns the name of the stream that this partition is reading from.
        :return: The name of the stream.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Closes the partition."""
        pass

    @abstractmethod
    def is_closed(self) -> bool:
        """Returns whether the partition is closed.
        :return:
        """
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Returns a hash of the partition.
        Partitions must be hashable so that they can be used as keys in a dictionary.
        """

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import itertools
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
from typing import Any, TypeVar

from airbyte_cdk.destinations.vector_db_based.document_processor import Chunk
from airbyte_cdk.models import AirbyteMessage, ConfiguredAirbyteCatalog


class Indexer(ABC):
    """
    Indexer is an abstract class that defines the interface for indexing documents.

    The Writer class uses the Indexer class to internally index documents generated by the document processor.
    In a destination connector, implement a custom indexer by extending this class and implementing the abstract methods.
    """

    def __init__(self, config: Any):  # noqa: ANN204, ANN401
        self.config = config
        pass

    def pre_sync(self, catalog: ConfiguredAirbyteCatalog) -> None:  # noqa: B027
        """
        Run before the sync starts. This method should be used to make sure all records in the destination that belong to streams with a destination mode of overwrite are deleted.

        Each record has a metadata field with the name airbyte_cdk.destinations.vector_db_based.document_processor.METADATA_STREAM_FIELD which can be used to filter documents for deletion.
        Use the airbyte_cdk.destinations.vector_db_based.utils.create_stream_identifier method to create the stream identifier based on the stream definition to use for filtering.
        """
        pass

    def post_sync(self) -> list[AirbyteMessage]:
        """
        Run after the sync finishes. This method should be used to perform any cleanup operations and can return a list of AirbyteMessages to be logged.
        """
        return []

    @abstractmethod
    def index(self, document_chunks: list[Chunk], namespace: str, stream: str) -> None:
        """
        Index a list of document chunks.

        This method should be used to index the documents in the destination. If page_content is None, the document should be indexed without the raw text.
        All chunks belong to the stream and namespace specified in the parameters.
        """
        pass

    @abstractmethod
    def delete(self, delete_ids: list[str], namespace: str, stream: str) -> None:
        """
        Delete document chunks belonging to certain record ids.

        This method should be used to delete documents from the destination.
        The delete_ids parameter contains a list of record ids - all chunks with a record id in this list should be deleted from the destination.
        All ids belong to the stream and namespace specified in the parameters.
        """
        pass

    @abstractmethod
    def check(self) -> str | None:
        """
        Check if the indexer is configured correctly. This method should be used to check if the indexer is configured correctly and return an error message if it is not.
        """
        pass


T = TypeVar("T")


def chunks(iterable: Iterable[T], batch_size: int) -> Generator[tuple[T, ...], None, None]:
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))

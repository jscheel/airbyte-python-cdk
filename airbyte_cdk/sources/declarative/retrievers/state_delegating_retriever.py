#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from dataclasses import dataclass

from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.declarative.incremental.declarative_cursor import DeclarativeCursor


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
@dataclass
class StateDelegatingRetriever:

    full_data_retriever: Retriever
    incremental_data_retriever: Retriever
    cursor: DeclarativeCursor

    def __getattr__(self, name):
        # Avoid delegation for these internal names.
        if name in {"full_data_retriever", "incremental_data_retriever", "cursor", "retriever", "state"}:
            return object.__getattribute__(self, name)
        # Delegate everything else to the active retriever.
        return getattr(self.retriever, name)

    def __setattr__(self, name, value):
        # For the internal attributes, set them directly on self.
        if name in {"full_data_retriever", "incremental_data_retriever", "cursor", "state"}:
            super().__setattr__(name, value)
        else:
            # Delegate setting attributes to the underlying retriever.
            setattr(self.retriever, name, value)

    @property
    def retriever(self):
        return self.incremental_data_retriever if self.cursor.get_stream_state() else self.full_data_retriever

    @property
    def state(self):
        return self.cursor.get_stream_state() if self.cursor else {}

    @state.setter
    def state(self, value) -> None:
        """State setter, accept state serialized by state getter."""
        if self.cursor:
            self.cursor.set_initial_state(value)

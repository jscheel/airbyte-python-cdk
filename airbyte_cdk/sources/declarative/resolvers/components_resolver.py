#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass
from typing import Any, Dict, Mapping, Optional, Type, Union, Iterable, List
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from deprecated.classic import deprecated


@dataclass(frozen=True)
class ComponentMappingDefinition:
    """Defines the key-value mapping configuration for a stream component."""

    field_path: List["InterpolatedString"]
    value: Union["InterpolatedString", str]
    value_type: Optional[Type[Any]]
    parameters: InitVar[Mapping[str, Any]]


@dataclass(frozen=True)
class ResolvedComponentMappingDefinition:
    """Represents a parsed and resolved component mapping for a stream configuration."""

    field_path: List["InterpolatedString"]
    value: "InterpolatedString"
    value_type: Optional[Type[Any]]
    parameters: InitVar[Mapping[str, Any]]


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class ComponentsResolver(ABC):
    """
    Abstract base class for resolving components in a stream template.
    """

    @abstractmethod
    def resolve_components(
        self, stream_template_config: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        """
        Maps and populates values into a stream template configuration.
        :param stream_template_config: The stream template with placeholders for components.
        :yields: The resolved stream config with populated values.
        """
        pass

#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from dataclasses import InitVar, dataclass
from typing import Any, Dict, Mapping, Optional, Type, Union, Iterable

from airbyte_cdk.sources.declarative.interpolation import InterpolatedBoolean, InterpolatedString


@dataclass(frozen=True)
class ComponentMappingDefinition:
    """Defines the key-value mapping configuration for a stream component."""

    key: str
    value: Union["InterpolatedString", str]
    value_type: Optional[Type[Any]]
    parameters: InitVar[Mapping[str, Any]]
    condition: str = ""


@dataclass(frozen=True)
class ResolvedComponentMappingDefinition:
    """Represents a parsed and resolved component mapping for a stream configuration."""

    key: str
    value: Union["InterpolatedString", str]
    value_type: Optional[Type[Any]]
    parameters: InitVar[Mapping[str, Any]]
    condition: Optional[Union["InterpolatedBoolean", str]] = ""


@dataclass
class ComponentsResolver:
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

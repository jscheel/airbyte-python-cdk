#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import InitVar, dataclass
from typing import Any, Union

from typing_extensions import deprecated

from airbyte_cdk.sources.declarative.interpolation import InterpolatedString  # noqa: TC001
from airbyte_cdk.sources.source import ExperimentalClassWarning


@dataclass(frozen=True)
class ComponentMappingDefinition:
    """Defines the configuration for mapping a component in a stream. This class specifies
    what field in the stream template should be updated with value, supporting dynamic interpolation
    and type enforcement."""

    field_path: list["InterpolatedString"]
    value: Union["InterpolatedString", str]
    value_type: type[Any] | None
    parameters: InitVar[Mapping[str, Any]]


@dataclass(frozen=True)
class ResolvedComponentMappingDefinition:
    """Defines resolved configuration for mapping a component in a stream. This class specifies
    what field in the stream template should be updated with value, supporting dynamic interpolation
    and type enforcement."""

    field_path: list["InterpolatedString"]
    value: "InterpolatedString"
    value_type: type[Any] | None
    parameters: InitVar[Mapping[str, Any]]


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class ComponentsResolver(ABC):
    """
    Abstract base class for resolving components in a stream template.
    """

    @abstractmethod
    def resolve_components(
        self, stream_template_config: dict[str, Any]
    ) -> Iterable[dict[str, Any]]:
        """
        Maps and populates values into a stream template configuration.
        :param stream_template_config: The stream template with placeholders for components.
        :yields: The resolved stream config with populated values.
        """
        pass

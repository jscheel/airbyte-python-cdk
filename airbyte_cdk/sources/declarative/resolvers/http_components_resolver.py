#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from copy import deepcopy
from dataclasses import InitVar, dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Iterable

from airbyte_cdk.sources.declarative.interpolation import InterpolatedBoolean, InterpolatedString
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.source import ExperimentalClassWarning
from airbyte_cdk.sources.types import Config
from airbyte_cdk.sources.declarative.resolvers.components_resolver import (
    ComponentsResolver,
    ComponentMappingDefinition,
    ResolvedComponentMappingDefinition,
)
from deprecated.classic import deprecated


@deprecated("This class is experimental. Use at your own risk.", category=ExperimentalClassWarning)
@dataclass
class HttpComponentsResolver(ComponentsResolver):
    """
    Resolves and populates stream templates with components fetched via an HTTP retriever.

    Attributes:
        retriever (Retriever): The retriever used to fetch data from an API.
        config (Config): Configuration object for the resolver.
        components_mapping (List[ComponentMappingDefinition]): List of mappings to resolve.
        parameters (InitVar[Mapping[str, Any]]): Additional parameters for interpolation.
    """

    retriever: Retriever
    config: Config
    components_mapping: List[ComponentMappingDefinition]
    parameters: InitVar[Mapping[str, Any]]
    _resolved_components: List[ResolvedComponentMappingDefinition] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        """
        Initializes and parses component mappings, converting them to resolved definitions.

        Args:
            parameters (Mapping[str, Any]): Parameters for interpolation.
        """
        for component_mapping in self.components_mapping:
            condition = component_mapping.condition or "True"

            if isinstance(component_mapping.value, (str, InterpolatedString)):
                interpolated_value = (
                    InterpolatedString.create(component_mapping.value, parameters=parameters)
                    if isinstance(component_mapping.value, str)
                    else component_mapping.value
                )
                self._resolved_components.append(
                    ResolvedComponentMappingDefinition(
                        key=component_mapping.key,
                        value=interpolated_value,
                        value_type=component_mapping.value_type,
                        condition=InterpolatedBoolean(condition=condition, parameters=parameters),
                        parameters=parameters,
                    )
                )
            else:
                raise ValueError(
                    f"Expected a string or InterpolatedString for value in mapping: {component_mapping}"
                )

    def _update_config(
        self,
        component_config: Dict[str, Any],
        target_key: str,
        target_value: Any,
        condition: Optional[InterpolatedBoolean],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Recursively updates the configuration dictionary for a specific key.

        Args:
            component_config (Dict[str, Any]): Component config to update.
            key (str): Target key to update.
            value (Any): Value to assign to the target key.
            condition (Optional[InterpolatedBoolean]): Condition for applying the update.

        Returns:
            Dict[str, Any]: Updated configuration dictionary.
        """
        kwargs["current_component_config"] = component_config
        should_update = condition.eval(self.config, **kwargs) if condition else True

        for key, value in component_config.items():
            if key == target_key and should_update:
                component_config[key] = target_value
            elif isinstance(value, dict):
                component_config[key] = self._update_config(value, key, value, condition, **kwargs)
            elif isinstance(value, list):
                component_config[key] = [
                    self._update_config(item, key, value, condition, **kwargs)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]

        return component_config

    def resolve_components(
        self, stream_template_config: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        """
        Resolves components in the stream template configuration by populating values.

        Args:
            stream_template_config (Dict[str, Any]): Stream template to populate.

        Yields:
            Dict[str, Any]: Updated configurations with resolved components.
        """
        kwargs = {"stream_template_config": stream_template_config}

        for components_values in self.retriever.read_records({}):
            updated_config = deepcopy(stream_template_config)
            kwargs["components_values"] = components_values  # type: ignore[assignment] # component_values will always be of type Mapping[str, Any]

            for resolved_component in self._resolved_components:
                valid_types = (
                    (resolved_component.value_type,) if resolved_component.value_type else None
                )
                value = resolved_component.value.eval(
                    self.config, valid_types=valid_types, **kwargs
                )
                updated_config = self._update_config(
                    updated_config,
                    target_key=resolved_component.key,
                    target_value=value,
                    condition=resolved_component.condition,  # type: ignore[arg-type]  # The condition in resolved_component always has the type InterpolatedBoolean if it exists.
                    **kwargs,
                )

            yield updated_config

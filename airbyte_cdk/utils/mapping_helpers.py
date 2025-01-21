#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import copy
from typing import Any, Dict, List, Mapping, Optional, Union


def _merge_mappings(
    target: Dict[str, Any],
    source: Mapping[str, Any],
    path: Optional[List[str]] = None,
) -> None:
    """
    Recursively merge two dictionaries, raising an error if there are any conflicts.
    A conflict occurs when the same path exists in both dictionaries with different values.

    Args:
        target: The dictionary to merge into
        source: The dictionary to merge from
        path: The current path in the nested structure (for error messages)
    """
    path = path or []
    for key, source_value in source.items():
        current_path = path + [str(key)]

        if key in target:
            target_value = target[key]
            if isinstance(target_value, dict) and isinstance(source_value, dict):
                # If both are dictionaries, recursively merge them
                _merge_mappings(target_value, source_value, current_path)
            elif target_value != source_value:
                # If same key has different values, that's a conflict
                raise ValueError(
                    f"Duplicate keys or nested path conflict found: {'.'.join(current_path)}"
                )
        else:
            # No conflict, just copy the value (using deepcopy for nested structures)
            target[key] = copy.deepcopy(source_value)


def combine_mappings(
    mappings: List[Optional[Union[Mapping[str, Any], str]]],
) -> Union[Mapping[str, Any], str]:
    """
    Combine multiple mappings into a single mapping, supporting nested structures.
    Raise errors in the following cases:
    * If there are multiple string mappings
    * If there is a string mapping AND any non-empty dictionary mappings
    * If there are conflicting paths across mappings (including nested conflicts)

    If there is exactly one string mapping and no other non-empty mappings, return that string.
    Otherwise, combine all dictionary mappings into a single mapping.
    """
    if not mappings:
        return {}

    # Count how many string options we have, ignoring None values
    string_options = sum(isinstance(mapping, str) for mapping in mappings if mapping is not None)
    if string_options > 1:
        raise ValueError("Cannot combine multiple string options")

    # Filter out None values and empty mappings
    non_empty_mappings = [
        m for m in mappings if m is not None and not (isinstance(m, Mapping) and not m)
    ]

    # If there is only one string option and no other non-empty mappings, return it
    if string_options == 1:
        if len(non_empty_mappings) > 1:
            raise ValueError("Cannot combine multiple options if one is a string")
        return next(m for m in non_empty_mappings if isinstance(m, str))

    # Start with an empty result and merge each mapping into it
    result: Dict[str, Any] = {}
    for mapping in non_empty_mappings:
        if mapping and isinstance(mapping, Mapping):
            _merge_mappings(result, mapping)

    return result

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Dict, List, Mapping, Optional, Union


def has_nested_conflict(path1: List[str], value1: Any, path2: List[str], value2: Any) -> bool:
    """
    Check if two paths conflict with each other.
    e.g. ["a", "b"] conflicts with ["a", "b"] if values differ
    e.g. ["a"] conflicts with ["a", "b"] (can't have both a value and a nested structure)
    """
    # If one path is a prefix of the other, they conflict
    shorter, longer = sorted([path1, path2], key=len)
    if longer[: len(shorter)] == shorter:
        return True

    # If paths are the same but values differ, they conflict
    if path1 == path2 and value1 != value2:
        return True

    return False


def flatten_mapping(
    mapping: Mapping[str, Any],
    prefix: Optional[List[str]] = None
) -> List[tuple[List[str], Any]]:
    """
    Convert a nested mapping into a list of (path, value) pairs
    e.g. {"a": {"b": 1}} -> [(["a", "b"], 1)]
    """
    prefix = prefix or []
    result = []

    for key, value in mapping.items():
        current_path = prefix + [key]
        if isinstance(value, Mapping):
            result.extend(flatten_mapping(value, current_path))
        else:
            result.append((current_path, value))

    return result


def combine_mappings(
    mappings: List[Optional[Union[Mapping[str, Any], str]]],
) -> Union[Mapping[str, Any], str]:
    """
    Combine multiple mappings into a single mapping, supporting nested structures.
    If any of the mappings are a string, return that string. Raise errors in the following cases:
    * If there are conflicting paths across mappings (including nested conflicts)
    * If there are multiple string mappings
    * If there are multiple mappings containing keys and one of them is a string
    """
    # Handle string cases first (maintaining existing behavior)
    string_options = sum(isinstance(mapping, str) for mapping in mappings if mapping is not None)
    if string_options > 1:
        raise ValueError("Cannot combine multiple string options")

    non_empty_mappings = [
        m for m in mappings if m is not None and not (isinstance(m, Mapping) and not m)
    ]

    if string_options == 1:
        if len(non_empty_mappings) > 1:
            raise ValueError("Cannot combine multiple options if one is a string")
        return next(m for m in non_empty_mappings if isinstance(m, str))

    # Convert all mappings to flat (path, value) pairs for conflict detection
    all_paths: List[List[tuple[List[str], Any]]] = []
    for mapping in mappings:
        if mapping is None or not isinstance(mapping, Mapping):
            continue
        all_paths.append(flatten_mapping(mapping))

    # Check for conflicts
    for i, paths1 in enumerate(all_paths):
        for path1, value1 in paths1:
            for paths2 in all_paths[i + 1 :]:
                for path2, value2 in paths2:
                    if has_nested_conflict(path1, value1, path2, value2):
                        raise ValueError(
                            f"Duplicate keys or nested path conflict found: {'.'.join(path1)} conflicts with {'.'.join(path2)}"
                        )

    # If no conflicts, merge all mappings
    result: Dict[str, Any] = {}
    for mapping in mappings:
        if mapping is None or not isinstance(mapping, Mapping):
            continue
        for path, value in flatten_mapping(mapping):
            current = result
            *prefix, last = path
            for key in prefix:
                current = current.setdefault(key, {})
            current[last] = value

    return result

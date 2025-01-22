# Copyright (c) 2023 Airbyte, Inc., all rights reserved.


from airbyte_cdk.sources.streams import Stream


def get_primary_key_from_stream(
    stream_primary_key: str | list[str] | list[list[str]] | None,
) -> list[str]:
    if stream_primary_key is None:
        return []
    if isinstance(stream_primary_key, str):
        return [stream_primary_key]
    if isinstance(stream_primary_key, list):
        are_all_elements_str = all(isinstance(k, str) for k in stream_primary_key)
        are_all_elements_list_of_size_one = all(
            isinstance(k, list) and len(k) == 1 for k in stream_primary_key
        )

        if are_all_elements_str:
            return stream_primary_key  # type: ignore # We verified all items in the list are strings
        if are_all_elements_list_of_size_one:
            return list(map(lambda x: x[0], stream_primary_key))  # noqa: C417, FURB118
        raise ValueError(f"Nested primary keys are not supported. Found {stream_primary_key}")
    raise ValueError(f"Invalid type for primary key: {stream_primary_key}")


def get_cursor_field_from_stream(stream: Stream) -> str | None:
    if isinstance(stream.cursor_field, list):
        if len(stream.cursor_field) > 1:
            raise ValueError(
                f"Nested cursor fields are not supported. Got {stream.cursor_field} for {stream.name}"
            )
        if len(stream.cursor_field) == 0:
            return None
        return stream.cursor_field[0]
    return stream.cursor_field

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Final


class SliceEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:  # noqa: ANN401
        if hasattr(obj, "__json_serializable__"):
            return obj.__json_serializable__()

        # Let the base class default method raise the TypeError
        return super().default(obj)


class SliceHasher:
    _ENCODING: Final = "utf-8"

    @classmethod
    def hash(cls, stream_name: str, stream_slice: Mapping[str, Any] | None = None) -> int:
        if stream_slice:
            try:
                s = json.dumps(stream_slice, sort_keys=True, cls=SliceEncoder)
                hash_input = f"{stream_name}:{s}".encode(cls._ENCODING)
            except TypeError as e:
                raise ValueError(f"Failed to serialize stream slice: {e}")  # noqa: B904
        else:
            hash_input = stream_name.encode(cls._ENCODING)

        # Use last 8 bytes as 64-bit integer for better distribution
        return int.from_bytes(hashlib.sha256(hash_input).digest()[-8:], "big")

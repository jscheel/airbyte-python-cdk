import hashlib
import json
import os
from pathlib import Path
from typing import Dict, Optional

from mitmproxy import ctx, http


class CacheAddon:
    def __init__(self) -> None:
        self.cache_dir = Path("/tmp/mitmproxy_cache")
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.cache: Dict[str, str] = {}

    def _get_cache_key(self, flow: http.HTTPFlow) -> str:
        """Generate a unique cache key for the request."""
        components = [
            flow.request.method,
            flow.request.pretty_url,
            (flow.request.content or b"").decode("utf-8", "ignore"),
            json.dumps(dict(flow.request.headers), sort_keys=True),
        ]
        return hashlib.sha256("".join(components).encode()).hexdigest()

    def _get_cached_response(self, key: str) -> Optional[http.Response]:
        """Retrieve cached response if it exists."""
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
                response = http.Response.make(
                    status_code=data["status_code"],
                    content=data["content"].encode(),
                    headers=data["headers"],
                )
                return response
        except Exception as e:
            getattr(ctx.log, "error")(f"Error reading cache: {e}")
            return None

    def _cache_response(self, key: str, flow: http.HTTPFlow) -> None:
        """Cache the response."""
        if not flow.response:
            return

        cache_file = self.cache_dir / f"{key}.json"
        try:
            data = {
                "status_code": flow.response.status_code,
                "content": (flow.response.content or b"").decode("utf-8", "ignore"),
                "headers": dict(flow.response.headers),
            }
            with open(cache_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            getattr(ctx.log, "error")(f"Error writing cache: {e}")

    def request(self, flow: http.HTTPFlow) -> None:
        """Check if request is cached and serve from cache if it exists."""
        if flow.request.method not in ["GET", "HEAD"]:
            return

        key = self._get_cache_key(flow)
        cached_response = self._get_cached_response(key)

        if cached_response:
            flow.response = cached_response
            getattr(ctx.log, "info")(f"Serving from cache: {flow.request.pretty_url}")

    def response(self, flow: http.HTTPFlow) -> None:
        """Cache successful responses."""
        if flow.request.method not in ["GET", "HEAD"]:
            return

        if flow.response and flow.response.status_code == 200:
            key = self._get_cache_key(flow)
            self._cache_response(key, flow)
            getattr(ctx.log, "info")(f"Cached response: {flow.request.pretty_url}")


addons = [CacheAddon()]

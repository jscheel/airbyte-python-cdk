import time


class ExpiringDictionary:
    def __init__(self, expiration_time: int):
        self._store = {}
        self._expiration_time = expiration_time

    def remove_expired_keys(self):
        """Remove expired keys from the dictionary."""
        current_time = time.time()
        expired_keys = []
        for key, (value, timestamp) in self._store.items():
            if current_time - timestamp >= self._expiration_time:
                expired_keys.append(key)
            else:
                # After Python 3.7, items will be presented in the order they were added.
                break
        for key in expired_keys:
            del self._store[key]
        self._store = dict(self._store)

    def set(self, key, value):
        self._store[key] = (value, time.time())

    def get(self, key):
        if key in self._store:
            value, _ = self._store[key]
            return value
        return None

    def __setitem__(self, key, value):
        self._store[key] = (value, time.time())

    def __getitem__(self, key):
        if key in self._store:
            value, _ = self._store[key]
            return value
        raise KeyError(f"Key '{key}' not found or expired.")

    def __contains__(self, key):
        return key in self._store

    def __len__(self):
        return len(self._store)

    def __repr__(self):
        with self._lock:
            return repr({key: value for key, (value, _) in self._store.items()})

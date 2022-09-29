from typing import Optional

from dynamodb_monotable.src.keys import Key, KeyType


class Index:
    def __init__(self, name: str, hash_key: Key, sort_key: Optional[Key]):
        self.name = name
        self.hash_key = hash_key
        self.sort_key = sort_key
        self._table = None

    # private api starts here.

    def _register_table(self, table) -> None:
        # check partion key
        if not isinstance(self.hash_key, Key):
            raise TypeError("Hash key must be a Key object.")
        if self.hash_key.key_type != KeyType.HASH:
            raise ValueError("Hash key must be a HASH key.")

        if not isinstance(self.sort_key, Key):
            raise TypeError("Sort key must be a Key object.")
        if self.sort_key & self.sort_key.key_type != KeyType.RANGE:
            raise ValueError(f"Sort key must be of type {KeyType.RANGE}")

        self._table = table
        return self

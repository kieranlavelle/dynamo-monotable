"""Holds a class describing the properties of a table.

This can be passed down to models that are requested from the table.
"""

from typing import List, Optional

from dynamodb_monotable.src.keys import Key
from dynamodb_monotable.src.index import Index


class TableProperties:
    def __init__(
        self,
        name: str,
        partition_key: Key,
        sort_key: Optional[Key],
        indexes: List[Index],
    ):
        self.name = name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.indexes = indexes

    def get_index(self, index_name: str) -> Optional[Index]:
        for index in self.indexes:
            if index.name == index_name:
                return index
        return None

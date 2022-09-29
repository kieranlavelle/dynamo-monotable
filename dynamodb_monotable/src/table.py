from typing import List, Optional, TypeVar

from dynamodb_monotable.src.keys import Key, KeyType
from dynamodb_monotable.src.items import Item
from dynamodb_monotable.src.index import Index
from dynamodb_monotable.src.table_properties import TableProperties

TypedModel = TypeVar("TypedModel")


class Table:
    def __init__(
        self,
        name: str,
        models: List[Item],
        partition_key: Key,
        sort_key: Optional[Key] = None,
        indexes: Optional[List[Index]] = None,
    ):
        self.name = name
        self.models = models
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.indexes = indexes or []

        # check that the partition & sort key are valid.
        self._validate_key_shema()

        # register all of the indexes with the table
        self.indexes = [index._register_table(self) for index in self.indexes]

    def get_model(self, requested_model: TypedModel) -> TypedModel:
        for model in self.models:
            if isinstance(model, requested_model):

                model_instance = model()
                model_instance.table_properties = TableProperties(
                    name=self.name,
                    partition_key=self.partition_key,
                    sort_key=self.sort_key,
                    indexes=self.indexes,
                )
                return model_instance
        return ValueError(f"Model {requested_model} not found.")

    # private api starts here.
    def _validate_key_shema(self):
        """Check that a valid key schema has been specified."""

        # check partion key
        if not isinstance(self.partition_key, Key):
            raise TypeError("Partition key must be a Key object.")
        if self.partition_key.key_type != KeyType.HASH:
            raise ValueError("Partition key must be a HASH key.")

        if not isinstance(self.sort_key, Key):
            raise TypeError("Partition key must be a Key object.")
        if self.sort_key & self.sort_key.key_type != KeyType.RANGE:
            raise ValueError(f"Sort key must be of type {KeyType.RANGE}")

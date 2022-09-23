import re
from typing import Dict, TypeVar, List, Optional, Type

from pydantic import BaseModel
import boto3

from dynamodb_monotable.table.indexes import Indexs
from dynamodb_monotable.table.attributes.attributes import Attribute
from dynamodb_monotable.table.models import Item

ModelName = TypeVar("ModelName", bound=str)
AttrName = TypeVar("AttrName", bound=str)
REPLACEMENT_PATTERN = re.compile(r"{[A-Za-z\d_]+}")
TypedModel = TypeVar("TypedModel")


class TableSchema(BaseModel):
    indexes: Indexs
    models: List[Type[Item]]

    def get_key_schema(self) -> List[Dict]:
        key_schema = []

        key_schema.append(
            {"AttributeName": self.indexes.primary.hash_key.name, "KeyType": "HASH"},
        )

        if self.indexes.primary.sort_key:
            key_schema.append(
                {
                    "AttributeName": self.indexes.primary.sort_key.name,
                    "KeyType": "RANGE",
                },
            )

        return key_schema

    def get_attribute_definitions(self) -> List[Dict]:
        attribute_definitions = []

        attribute_definitions.append(
            {
                "AttributeName": self.indexes.primary.hash_key.name,
                "AttributeType": self.indexes.primary.hash_key.type_,
            }
        )

        # add the range key definition if there is one
        if self.indexes.primary.sort_key:
            attribute_definitions.append(
                {
                    "AttributeName": self.indexes.primary.sort_key.name,
                    "AttributeType": self.indexes.primary.sort_key.type_,
                }
            )

        return attribute_definitions


class Table:
    def __init__(self, name: str, schema: TableSchema, client_config: Dict = None):
        self.name = name
        self.schema = schema
        self.client_config = client_config if client_config else {}

        self.table_config = {
            "table_name": self.name,
            "key_schema": {
                "hash_key": self.schema.indexes.primary.hash_key,
                "sort_key": self.schema.indexes.primary.sort_key,
            },
        }

    def create_table(self) -> None:
        client = boto3.client("dynamodb", **self.client_config)
        client.create_table(
            TableName=self.name,
            KeySchema=self.schema.get_key_schema(),
            AttributeDefinitions=self.schema.get_attribute_definitions(),
            BillingMode="PAY_PER_REQUEST",
        )

    def delete_table(self) -> None:
        client = boto3.client("dynamodb", **self.client_config)
        client.delete_table(TableName=self.name)

    def get_model(self, model_class: TypedModel) -> TypedModel:

        try:
            model = [m for m in self.schema.models if m == model_class]
            model = model[0]
            return model(self.table_config, self.client_config)
        except IndexError:
            raise ValueError(f"Model of type {model_class} does not exist")

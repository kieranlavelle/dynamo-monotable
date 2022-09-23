import re
from typing import Dict, TypeVar, List, Optional, Type

from pydantic import BaseModel
import boto3

from dynamodb_monotable.table.indexes import Indexs
from dynamodb_monotable.table.attributes.attributes import ModelAttribute
from dynamodb_monotable.table.models import BaseItem

ModelName = TypeVar("ModelName", bound=str)
AttrName = TypeVar("AttrName", bound=str)
REPLACEMENT_PATTERN = re.compile(r"{[A-Za-z\d_]+}")


class TableSchema(BaseModel):
    indexes: Indexs
    models: Dict[AttrName, Type[BaseItem]]

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
    def __init__(self, name: str, schema: TableSchema):
        self.name = name
        self.schema = schema

    def create_table(self, *, endpoint_url: Optional[str] = None) -> None:
        client = boto3.client("dynamodb", endpoint_url=endpoint_url)
        client.create_table(
            TableName=self.name,
            KeySchema=self.schema.get_key_schema(),
            AttributeDefinitions=self.schema.get_attribute_definitions(),
            BillingMode="PAY_PER_REQUEST",
        )

    def get_model(self, model_name: ModelName) -> Dict[AttrName, ModelAttribute]:

        try:
            return self.schema.models[model_name]
        except KeyError:
            return ValueError(f"Model {model_name} does not exist")

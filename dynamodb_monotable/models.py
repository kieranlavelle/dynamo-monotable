from typing import Dict, ClassVar, List, Any, Optional, TypeVar
from dataclasses import dataclass, fields, field

import boto3

from dynamodb_monotable.resolvers import solve_template_values
from dynamodb_monotable.attributes import Attribute
from dynamodb_monotable import query_engine

TItem = TypeVar("TItem", bound="Item")


@dataclass
class Item:

    ignored_fields: ClassVar[List[str]] = [
        "table_config",
        "client_config",
        "create_state",
        "attribute_values",
    ]

    table_config: Dict[str, Any]
    client_config: Dict[str, Any]
    create_state: bool = False
    attribute_values: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """We use post_init to set the values of the names in the Attribute class."""
        # set the field names of the attr's after init
        for field in fields(self):
            if field.name not in self.ignored_fields:
                attribute = getattr(self, field.name)
                attribute.name = field.name

    def _field_names(self) -> List[str]:
        return [f.name for f in fields(self) if f.name not in self.ignored_fields]

    def _fields(self) -> Dict[str, Attribute]:
        return {
            f.name: f.default for f in fields(self) if f.name not in self.ignored_fields
        }

    def _validate_model(self):
        # check hash key and optionally sort key are on the model.
        hash_key = self.table_config["key_schema"]["hash_key"]
        sort_key = self.table_config["key_schema"]["sort_key"]
        if hash_key.name not in self._field_names():
            raise ValueError(f"{hash_key.name} is not on the model.")
        if sort_key and sort_key.name not in self._field_names():
            raise ValueError(f"{sort_key.name} is not on the model.")

    def save(self, **kwargs) -> None:
        if self.create_state is False:
            raise ValueError("Item must be created before saving.")

        dynamodb = boto3.resource("dynamodb", **self.client_config)
        table = dynamodb.Table(self.table_config["table_name"])

        item = {}
        for field_name, field in self._fields().items():
            value = self.attribute_values[field_name]
            serialized_value = field.serialize(value)
            item[field_name] = serialized_value

        table.put_item(Item=item, **kwargs)

    def create(self, **values) -> TItem:
        self.attribute_values.update(values)

        # check the hk and sk are provided if needed
        self._validate_model()

        # populate item attributes.
        resolved_fields = solve_template_values(self._fields(), values)
        self.attribute_values.update(resolved_fields)

        self.create_state = True
        return self

    def get(self) -> TItem:
        if not self.create_state:
            raise ValueError("Item must be created before you can call `get`.")

        hash_key = getattr(self, self.table_config["key_schema"]["hash_key"].name)
        sort_key = None
        if self.table_config["key_schema"]["sort_key"]:
            sort_key = getattr(self, self.table_config["key_schema"]["sort_key"].name)

        return self.get_by_key(hash_key.value, sort_key.value)

    def get_by_key(self, hash_key: Any, sort_key: Optional[Any] = None) -> TItem:
        dynamodb = boto3.resource("dynamodb", **self.client_config)
        table = dynamodb.Table(self.table_config["table_name"])

        key = {self.table_config["key_schema"]["hash_key"].name: hash_key}
        if sort_key:
            if self.table_config["key_schema"]["sort_key"]:
                key[self.table_config["key_schema"]["sort_key"].name] = sort_key
            else:
                raise ValueError(
                    "Sort key is not defined on the table but has been provided."
                )

        # TODO: this is basic at the moment and can support far more args.
        response = table.get_item(Key=key)
        if "Item" not in response:
            raise ValueError("Item not found.")

        response_item: Dict[str, Any] = response["Item"]
        for field_name, field in self._fields().items():
            if field_name in response_item:
                setattr(self, field_name, field.deserialize(response_item[field_name]))
            else:
                # TODO: What if they dont want to return all fields? Should we move values out of field definition?
                setattr(self, field_name, None)

        return self

    def query(self, hash_key: Any, key_condition: Optional[List[Dict]]) -> List[TItem]:

        # expressions = []
        # values = {}

        # # get the hash key name
        # hk_placeholder = f":hk"
        # hash_key_name = self.table_config["key_schema"]["hash_key"].name
        # hash_key_type = self.table_config["key_schema"]["hash_key"].type_
        # hk_expression = f"{hash_key_name} = {hk_placeholder}"
        # exp = {hk_placeholder: {hash_key_type: hash_key}}

        # expressions.append(hk_expression)
        # values.update(exp)

        # # if key_condition is specified check the table has a sort key.
        # if key_condition and not self.table_config["key_schema"]["sort_key"]:
        #     raise ValueError(
        #         "Sort key is not defined on the table but a key-condition has been provided."
        #     )
        # expressions.append(key_condition["exp"])
        # values.update(key_condition["values"])

        # # craft the query
        # key_condition = " AND ".join(expressions)

        expression, values = query_engine.create_key_condition(
            self.table_config, hash_key, key_condition
        )

        client = boto3.client("dynamodb", **self.client_config)
        response = client.query(
            TableName=self.table_config["table_name"],
            Select="ALL_ATTRIBUTES",
            KeyConditionExpression=expression,
            ExpressionAttributeValues=values,
        )

        # TODO: Need to parse the response here...
        x = 1
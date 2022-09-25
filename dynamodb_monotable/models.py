from typing import Dict, ClassVar, List, Any, Optional, TypeVar, Iterator
from dataclasses import dataclass, fields, field

import boto3

from dynamodb_monotable.resolvers import solve_template_values
from dynamodb_monotable.attributes import Attribute, Condition

TItem = TypeVar("TItem", bound="Item")


class NoArguemnt:
    pass


class ResultsSet:
    def __init__(
        self,
        model: TItem,
        response: Dict[str, Any],
    ):
        self._model = model
        self.last_evaluated_key = response.get("LastEvaluatedKey", {})
        self.count = response["Count"]
        self._results = response["Items"]
        self._last_iterated_index = 0

    def __next__(self) -> TItem:
        try:
            item = self._results[self._last_iterated_index]
            deserialized_item = {}
            for field_name, field in self._model._fields().items():
                field_value = item[field_name][field.dynamodb_type]
                deserialized_field_value = field.deserialize(field_value)
                deserialized_item[field_name] = deserialized_field_value

            self._last_iterated_index += 1
            return self._model.__class__(
                table_config=self._model.table_config,
                client_config=self._model.client_config,
                create_state=True,
                attribute_values=deserialized_item,
            )
        except IndexError:
            raise StopIteration

    def __iter__(self) -> Iterator[TItem]:
        return self


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

    def hash_key(self, index: str = "primary") -> Attribute:
        hash_key_name = self.table_config["indexes"][index].hash_key.name
        return self._fields()[hash_key_name]

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

        # check for default fields
        for field_name, field in self._fields().items():
            if field_name not in self.attribute_values:
                self.attribute_values[field_name] = field.default

        # check for required fields
        for field_name, field in self._fields().items():
            if field_name not in self.attribute_values and field.required:
                raise ValueError(f"{field_name} is required.")

        # check the hk and sk are provided if needed
        self._validate_model()

        # populate item attributes.
        resolved_fields = solve_template_values(self._fields(), values)
        self.attribute_values.update(resolved_fields)

        self.create_state = True
        return self

    def get(self, index_name: str = "primary") -> TItem:
        if not self.create_state:
            raise ValueError("Item must be created before you can call `get`.")

        index = self.table_config["indexes"][index_name]

        hash_key = self.attribute_values[index.hash_key.name]
        sort_key = None
        if index.sort_key:
            sort_key = self.attribute_values[index.sort_key.name]

        return self.get_by_key(hash_key, sort_key)

    def get_by_key(
        self,
        hash_key: Any,
        sort_key: Optional[Any] = None,
        index_name: str = "primary",
    ) -> TItem:

        # TODO: Need's updating to work with index...
        # update the key schema so primary is named primary...
        index = self.table_config["indexes"][index_name]

        dynamodb = boto3.resource("dynamodb", **self.client_config)
        table = dynamodb.Table(self.table_config["table_name"])

        key = {index.hash_key.name: hash_key}
        if sort_key:
            if index.sort_key:
                key[index.sort_key.name] = sort_key
            else:
                raise ValueError(
                    "Sort key is not defined on the table but has been provided."
                )

        # TODO: this is basic at the moment and can support far more args.
        # TODO: Also need to support a number of exceptions
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

    def query(
        self,
        hash_key: Any,
        key_condition: Condition = Condition(),
        filter_expression: Condition = Condition(),
        limit: Optional[int] = None,
        index: str = "primary",
    ) -> ResultsSet:

        # TODO: Update query to support index

        key_condition = self.hash_key(index).eq(hash_key) & key_condition
        query_arguments = {
            "KeyConditionExpression": key_condition.expression,
            "ExpressionAttributeValues": {
                **key_condition.values,
                **filter_expression.values,
            },
            "IndexName": index if index != "primary" else NoArguemnt(),
            "Limit": limit if limit else NoArguemnt(),
            "FilterExpression": filter_expression.expression
            if filter_expression
            else NoArguemnt(),
            "Select": "ALL_ATTRIBUTES",
            "TableName": self.table_config["table_name"],
        }

        # remove the arguments where no argument was provided.
        query_arguments = {
            k: v for k, v in query_arguments.items() if not isinstance(v, NoArguemnt)
        }

        # TODO: Update this so it automatically fetches more results for us if a limit is not provided.
        client = boto3.client("dynamodb", **self.client_config)
        response = client.query(**query_arguments)

        return ResultsSet(model=self, response=response)

from typing import Optional, Any, Tuple, Dict
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4


class Condition:
    def __init__(self, expression: str = "", values: Optional[Dict[str, Any]] = None):
        self.expression = expression
        self.values = values if values else {}

    def __and__(a, b):
        return Condition(f"{a.expression} AND {b.expression}", {**a.values, **b.values})

    def __or__(a, b):
        return Condition(f"{a.expression} OR {b.expression}", {**a.values, **b.values})


class Attribute(ABC):
    def __init__(
        self,
        name: Optional[str] = None,
        value: Optional[Any] = None,
        required: bool = False,
        default: Optional[Any] = None,
    ):
        self.name = name
        self.value = value
        self.required = required
        self.default = default

    @abstractmethod
    def serialize(self, value: Any) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, value: Any) -> Any:
        raise NotImplementedError()

    @property
    @abstractmethod
    def dynamodb_type(self) -> str:
        raise NotImplementedError()

    def begins_with(self, value: Any) -> Condition:
        placeholder = f":{self.name}"
        key_condition_expression_part = f"begins_with({self.name}, {placeholder})"
        expression_attribute_values_part = {
            f"{placeholder}": {self.dynamodb_type: self.serialize(value)}
        }

        return Condition(
            expression=key_condition_expression_part,
            values=expression_attribute_values_part,
        )

    def eq(self, value: Any) -> Condition:
        placeholder = f":{uuid4().hex}"
        expression = f"{self.name} = {placeholder}"
        values = {f"{placeholder}": {self.dynamodb_type: self.serialize(value)}}

        return Condition(expression=expression, values=values)

    def gt(self, value: Any) -> Condition:
        placeholder = f":{uuid4().hex}"
        expression = f"{self.name} > {placeholder}"
        values = {f"{placeholder}": {self.dynamodb_type: self.serialize(value)}}

        return Condition(expression=expression, values=values)

    def lt(self, value: Any) -> Condition:
        placeholder = f":{uuid4().hex}"
        expression = f"{self.name} < {placeholder}"
        values = {f"{placeholder}": {self.dynamodb_type: self.serialize(value)}}

        return Condition(expression=expression, values=values)


class StringAttribute(Attribute):

    dynamodb_type = "S"

    def serialize(self, value: Any) -> str:
        return str(value)

    def deserialize(self, value: str) -> str:
        return value


class IntAttribute(Attribute):

    dynamodb_type = "N"

    def serialize(self, value: Any) -> int:
        return int(value)

    def deserialize(self, value: int) -> int:
        return int(value)


class UTCDateTimeAttribute(StringAttribute):
    def serialize(self, value: datetime) -> str:
        return value.isoformat()

    def deserialize(self, value: str) -> datetime:
        return datetime.fromisoformat(value)

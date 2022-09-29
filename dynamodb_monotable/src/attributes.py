from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Any


class AttributeDescriptor:
    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.attribute_values.get(self._name, None)

    def __set__(self, instance, value):
        if instance:
            instance.attribute_values[self._name] = value


class Attribute(ABC, AttributeDescriptor):
    def __init__(
        self,
        template: Optional[str] = None,
        required: bool = False,
        default: Optional[Any] = None,
    ):
        self.template = template
        self.required = required
        self.default = default

    @property
    @abstractmethod
    def dynamodb_type(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, value: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, value: Any) -> Any:
        raise NotImplementedError


class StringAttribute(Attribute):

    dynamodb_type = "S"

    def serialize(self, value: Any) -> str:
        return str(value)

    def deserialize(self, value: Any) -> str:
        return str(value)


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

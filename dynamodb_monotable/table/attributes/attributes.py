from typing import Optional, Any
from abc import ABC, abstractmethod
from datetime import datetime


class Attribute(ABC):
    def __init__(
        self,
        value: Optional[Any] = None,
        required: bool = False,
        default: Optional[Any] = None,
    ):
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
        return value


class UTCDateTimeAttribute(StringAttribute):
    def serialize(self, value: datetime) -> str:
        return value.isoformat()

    def deserialize(self, value: str) -> datetime:
        return datetime.fromisoformat(value)

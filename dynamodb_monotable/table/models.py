from typing import Dict, Any
from numbers import Number

from pydantic import BaseModel
from pydantic.fields import ModelField

from dynamodb_monotable.table.attributes import resolvers
from dynamodb_monotable.table.attributes.attributes import ModelAttribute


class BaseItem(BaseModel):
    def validate_model(self) -> None:

        for field_name in self.dict().keys():
            field = getattr(self, field_name)

            # if there is not a valid value raise an error
            if field.required and not (field.value or field.default):
                raise ValueError(
                    f"Field {field_name} is required but was not provided."
                )

            # set the default value if it is set.
            if field.value is None and field.default:
                setattr(self, field_name, field.default)

            if field.type_ == "S" and not isinstance(field.value, str):
                raise ValueError(f"Field {field_name} must be a string.")
            if field.type_ == "N" and not isinstance(field.value, Number):
                raise ValueError(f"Field {field_name} must be a number.")

    def save(self, client) -> None:
        self.validate_model()
        # TODO: Persist the model

    @staticmethod
    def get():
        pass

    @staticmethod
    def query():
        pass

    @classmethod
    def create(cls, **values) -> "BaseItem":
        attrs: Dict[str, ModelField] = cls.__fields__
        attrs: Dict[str, ModelAttribute] = {k: v.default for k, v in attrs.items()}
        resolved_attrs = resolvers.resolve_values(attrs, values)

        # check there are no unresolved values and raise an error if there are...

        return cls(**resolved_attrs)

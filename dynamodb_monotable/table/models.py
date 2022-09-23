from typing import Dict, ClassVar, List, Any
from dataclasses import dataclass, fields
from numbers import Number

import boto3


from dynamodb_monotable.table.attributes import resolvers
from dynamodb_monotable.table.attributes.attributes import Attribute


@dataclass
class Item:

    ignored_fields: ClassVar[List[str]] = [
        "table_config",
        "client_config",
        "create_state",
    ]

    table_config: Dict[str, Any]
    client_config: Dict[str, Any]
    create_state: bool = False

    def create(self, **values) -> "Item":
        for k, v in values.items():
            setattr(self, k, v)

        # check the hk and sk are provided if needed
        self._validate_model()

        # populate item attributes.
        resolved_fields = resolvers.resolve_values(self._fields(), values)
        for k, v in resolved_fields.items():
            setattr(self, k, v)

        self.create_state = True
        return self

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

        table.put_item(
            Item={k: v.serialize(v.value) for k, v in self._fields().items()}, **kwargs
        )


# class BaseItem(BaseModel):
#     class Config:
#         extra = "allow"

#     def validate_model(self) -> None:

#         for field_name, field in self._iter_items():

#             # if there is not a valid value raise an error
#             if field.required and not (field.value or field.default):
#                 raise ValueError(
#                     f"Field {field_name} is required but was not provided."
#                 )

#             # set the default value if it is set.
#             if field.value is None and field.default:
#                 setattr(self, field_name, field.default)

#             if field.type_ == "S" and not isinstance(field.value, str):
#                 raise ValueError(f"Field {field_name} must be a string.")
#             if field.type_ == "N" and not isinstance(field.value, Number):
#                 raise ValueError(f"Field {field_name} must be a number.")

#     def _iter_items(self) -> Tuple[str, ModelAttribute]:
#         for field_name in self.__fields__.keys():
#             yield field_name, getattr(self, field_name)

#     def save(self, **kwargs) -> None:
#         self.validate_model()
#         dynamodb = boto3.resource("dynamodb", **self.client_config)
#         table = dynamodb.Table(self.table_name)

#         table.put_item(Item={k: v.value for k, v in self._iter_items()}, **kwargs)

#     @staticmethod
#     def get(hash_key: Any, sort_key: Optional[Any], **kwargs) -> BaseModel:

#         dynamodb = boto3.resource("dynamodb", **BaseItem.client_config)
#         table = dynamodb.Table(BaseItem.table_name)

#         # need a way to get the hash key name and sort key name.
#         key = {"hk": kwargs["hk"], "sk": kwargs["sk"]}

#     @staticmethod
#     def query():
#         pass

#     @classmethod
#     def create(cls, **values) -> "BaseItem":
#         attrs: Dict[str, ModelField] = cls.__fields__
#         attrs: Dict[str, ModelAttribute] = {k: v.default for k, v in attrs.items()}
#         resolved_attrs = resolvers.resolve_values(attrs, values)

#         # check there are no unresolved values and raise an error if there are...

#         base_item = cls(**resolved_attrs)
#         # copy the config onto the class
#         base_item.client_config = cls.client_config
#         base_item.table_name = cls.table_name
#         return base_item

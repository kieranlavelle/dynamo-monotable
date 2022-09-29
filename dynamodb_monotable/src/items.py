from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from dynamodb_monotable.src.attributes import Attribute, StringAttribute
from dynamodb_monotable.src.table_properties import TableProperties


class ItemMetaClass(type):
    def __new__(cls, name, bases, attrs):
        attributes = {}
        attribute_values = {}

        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, Attribute):
                attributes[attr_name] = attr_value

                # set the default values on the item
                if attr_value.default is not None:
                    attribute_values[attr_name] = attr_value.default

        attrs["attributes"] = attributes
        attrs["attribute_values"] = attribute_values

        return super().__new__(cls, name, bases, attrs)


class Item(metaclass=ItemMetaClass):
    def index(self, index_name: Optional[str] = None) -> "Item":
        # this should actually set an Index class.
        self._current_index = index_name
        return self

    def get(
        self, hash_key: Optional[Any] = None, sort_key: Optional[Any] = None, **kwargs
    ):

        # if they dont provide a hk or sk we find the hk & sk names and check kwargs.
        sort_key_value = sort_key if sort_key else None
        hash_key_value = hash_key if hash_key else None

        try:
            if self.sort_key() & sort_key is None:
                sort_key_value = kwargs[self.sort_key().name]
        except KeyError:
            raise ValueError("Sort key not provided explicity, or in kwargs.")

        try:
            if self.hash_key() & hash_key is None:
                hash_key_value = kwargs[self.hash_key().name]
        except KeyError:
            raise ValueError("Hash key not provided explicity, or in kwargs.")

        # do get on asyncioboto

        # remove any index that was set.
        self._clear_index()

    def put(self, item: Dict[str, Any]) -> None:
        pass

    # properties start here
    @property
    def sort_key(self) -> Optional[Attribute]:

        # get the sort key's name
        key_name = None
        if self._current_index:
            index = self.table_properties.get_index(self._current_index)
            if index:
                key_name = index.sort_key.name if index.sort_key else None
        else:
            key_name = (
                self.table_properties.sort_key.name
                if self.table_properties.sort_key
                else None
            )

        # return the attribute for the sort key
        return self.attributes.get(key_name, None)

    @property
    def hash_key(self) -> Optional[Attribute]:

        # get the sort key's name
        key_name = None
        if self._current_index:
            index = self.table_properties.get_index(self._current_index)
            if index:
                key_name = index.hash_key.name
        else:
            key_name = self.table_properties.hash_key.name

        # return the attribute for the hash key
        return self.attributes.get(key_name, None)

    @property
    def table_properties(self) -> TableProperties:
        raise self._table_properties

    @table_properties.setter
    def table_properties(self, table_properties: TableProperties) -> None:
        self._table_properties = table_properties

    # private api starts here.

    def _clear_index(self) -> None:
        self._current_index = None


class Workorder(Item):
    id = StringAttribute(required=True)
    name = StringAttribute(required=True)
    description = StringAttribute(required=True)


x = Workorder()
x.index("work_order_by_task").get("123", "456")

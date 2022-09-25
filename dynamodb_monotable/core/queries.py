"""Holds the core logic for handling queries."""

from typing import Any, Union, Optional

from dynamodb_monotable.attributes import Condition

sentinal = object()
TOptional = Union[object, Condition]


def create_key_condition(
    hash_key_value: Any, key_condition: TOptional = sentinal, index: str = "primary"
) -> Condition:

    key_condition = self.hash_key(index).eq(hash_key_value) & key_condition


def parse_args(
    hash_key: Any,
    key_condition: TOptional = sentinal,
    filter_expression: TOptional = sentinal,
    limit: Optional[int] = None,
    index: str = "primary",
):
    """Parse the arguments passed to the query."""

    key_condition = create_key_condition(hash_key, key_condition, index)

"""Holds the core logic for handling queries."""

from typing import Any, Union, Optional, Dict

from dynamodb_monotable.attributes import Condition, Attribute


def create_key_condition(
    hash_key: Attribute,
    hash_key_value: Any,
    key_condition: Optional[Condition] = None,
) -> Condition:
    """Create the key condition for the query.

    Args:
        hash_key: The hash key for the query. Scoped to an index.
        hash_key_value: The value for the hash key.
        key_condition: The key condition for the query. (condition on the sort key.)
    """

    if not key_condition:
        return hash_key.eq(hash_key_value)
    return hash_key.eq(hash_key_value) & key_condition


def parse_query_args(
    hash_key: Attribute,
    hash_key_value: Any,
    table_name: str,
    key_condition: Optional[Condition] = None,
    filter_expression: Optional[Condition] = None,
    limit: Optional[int] = None,
    index: Optional[str] = None,
    select: Optional[str] = None,
    consistent_read: bool = False,
    scan_index_forward: bool = True,
    exclusive_start_key: Optional[dict] = None,
    return_consumed_capacity: Optional[str] = None,
    projection_expression: Optional[str] = None,
) -> Dict[str, Any]:
    """Parse the arguments passed to the query."""

    key_condition = create_key_condition(hash_key, hash_key_value, key_condition)
    filter_expression_values = filter_expression.values if filter_expression else {}
    filter_expression = filter_expression.expression if filter_expression else None

    args = {
        "TableName": table_name,
        "KeyConditionExpression": key_condition.expression,
        "ExpressionAttributeValues": {
            **key_condition.values,
            **filter_expression_values,
        },
        "FilterExpression": filter_expression,
        "Limit": limit,
        "Index": index,
        "Select": select,
        "ConsistentRead": consistent_read,
        "ScanIndexForward": scan_index_forward,
        "ExclusiveStartKey": exclusive_start_key,
        "ReturnConsumedCapacity": return_consumed_capacity,
        "ProjectionExpression": projection_expression,
    }

    return {
        argument: argument_value
        for argument, argument_value in args.items()
        if argument_value is not None
    }

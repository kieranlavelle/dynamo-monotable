from typing import Dict, Optional, Any, Tuple, List


def _create_hash_key_expression(table_config: Dict, hash_key: str) -> Tuple[str, Dict]:
    hk_placeholder = f":hk"
    hash_key_name = table_config["key_schema"]["hash_key"].name
    hash_key_type = table_config["key_schema"]["hash_key"].type_

    expression = f"{hash_key_name} = {hk_placeholder}"
    values = {hk_placeholder: {hash_key_type: hash_key}}

    return expression, values


def _create_key_condition_expression(
    table_config: Dict, key_condition: Optional[Any] = None
) -> Tuple[str, Dict]:
    """Create a key condition expression for a query"""

    _validate_key_condition(table_config, key_condition)
    if not key_condition:
        return "", {}

    x = 1


def create_key_condition(
    table_config: Dict, hash_key: str, key_condition: Optional[Any] = None
):
    """Create a key condition for a query"""

    expression = []
    expression_values = {}

    hk_expression, hk_values = _create_hash_key_expression(table_config, hash_key)
    kc_expression, kc_values = _create_key_condition_expression(
        table_config, key_condition
    )


def _validate_key_condition(table_config: Dict, key_condition: Optional[Any] = None):
    if key_condition and not table_config["key_schema"]["sort_key"]:
        raise ValueError(
            "Sort key is not defined on the table but a key-condition has been provided."
        )
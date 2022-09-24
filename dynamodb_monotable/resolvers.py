"""Module to hold all of the code for resolving attribute values."""

import re
from typing import List, Dict, Any

from dynamodb_monotable.attributes import Attribute

REPLACEMENT_PATTERN = re.compile(r"\${(?P<name>[A-Za-z\d_]+)}")


def _get_replacement_fields(v: str, /) -> List[str]:
    if not isinstance(v, str):
        return []
    return REPLACEMENT_PATTERN.findall(v)


def _field_count(v: str, /) -> int:
    return len(_get_replacement_fields(v))


def _try_format_values(format_string: str, values: Dict[str, str]) -> str:
    for k, v in values.items():
        format_string = format_string.replace(f"${{{k}}}", str(v))
    return format_string


def _create_key_to_field_count(attributes: List[Attribute]) -> dict:
    return {
        attr.name: _field_count(attr.value)
        for attr in sorted(attributes, key=lambda item: _field_count(item.value))
    }


def _replace_placeholders(
    field_by_replacement_count: Dict[str, int],
    attributes: List[Attribute],
    values: Dict[str, Any],
) -> Dict[str, str]:
    lowest_replacements = min(field_by_replacement_count.values())
    replacements = {
        k for k, v in field_by_replacement_count.items() if v == lowest_replacements
    }

    for attr_name in replacements:
        unformatted_placeholder_string = [a for a in attributes if a.name == attr_name][
            0
        ].value
        formatted_value = _try_format_values(unformatted_placeholder_string, values)
        values.update({attr_name: formatted_value})

    return values


def solve_template_values(
    fields: Dict[str, Attribute], values: Dict[str, Any]
) -> Dict[str, Any]:

    # check if there are any fields we need to solve.
    fields_to_solve = [f for f in fields.values() if f.name not in values]
    if not fields_to_solve:
        return values

    fields_by_replacements_count = _create_key_to_field_count(fields_to_solve)
    values = _replace_placeholders(
        fields_by_replacements_count, fields_to_solve, values
    )

    # values.update({k: v.value for k, v in fields.items() if _field_count(v.value) == 0})
    return solve_template_values(fields, values)

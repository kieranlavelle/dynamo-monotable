"""Module to hold all of the code for resolving attribute values."""

import re
from typing import List, Dict, Any

from dynamodb_monotable.table.attributes.attributes import Attribute

REPLACEMENT_PATTERN = re.compile(r"\${(?P<name>[A-Za-z\d_]+)}")


def _get_replacement_fields(v: str, /) -> List[str]:
    if not isinstance(v, str):
        return []
    return REPLACEMENT_PATTERN.findall(v)


def _field_count(v: str, /) -> int:
    return len(_get_replacement_fields(v))


def _try_format_values(format_string: str, values: Dict[str, str]) -> str:
    for k, v in values.items():
        format_string = format_string.replace(f"${{{k}}}", str(v.value))
    return format_string


def _create_key_to_field_count(model: Dict[str, str]) -> dict:
    return {
        k: _field_count(v.value)
        for k, v in sorted(model.items(), key=lambda item: _field_count(item[1].value))
    }


def _replace_placeholders(
    field_by_replacement_count: dict, model: Dict[str, str]
) -> Dict[str, str]:
    lowest_replacements = min(field_by_replacement_count.values())
    replacements = {
        k for k, v in field_by_replacement_count.items() if v == lowest_replacements
    }

    for attr_name in replacements:
        attr_value = model[attr_name].value
        model[attr_name].value = _try_format_values(attr_value, model)

    return model


def resolve_values(
    fields: Dict[str, Attribute], values: Dict[str, Any]
) -> Dict[str, str]:

    # pre-fill all of the values that we can
    shortened_model = fields.copy()
    for k, v in values.items():
        fields[k].value = v
        del shortened_model[k]
        if not shortened_model:
            return fields

    fields_by_replacements_count = _create_key_to_field_count(shortened_model)
    fields = _replace_placeholders(fields_by_replacements_count, fields)

    values.update({k: v.value for k, v in fields.items() if _field_count(v.value) == 0})
    return resolve_values(fields, values)

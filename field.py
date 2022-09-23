import re
from typing import List

REPLACEMENT_PATTERN = re.compile(r"\${(?P<name>[A-Za-z\d_]+)}")

fields = {
    "pk": "#ORG:${org_id}#WO:${wo_id}",
    "org_id": "",
    "wo_id": "${id}-12323",
    "id": "",
}

values = {"org_id": 123, "id": 5453}


def _get_replacement_fields(v: str, /) -> List[str]:
    if not isinstance(v, str):
        return []
    return REPLACEMENT_PATTERN.findall(v)


def _field_count(v: str, /) -> int:
    return len(_get_replacement_fields(v))


def _try_format_values(format_string: str, values: dict) -> str:
    for k, v in values.items():
        format_string = format_string.replace(f"${{{k}}}", str(v))
    return format_string


def _create_key_to_field_count(model: dict) -> dict:
    return {
        k: _field_count(v)
        for k, v in sorted(model.items(), key=lambda item: _field_count(item[1]))
    }


def _replace_placeholders(field_by_replacement_count: dict, model: dict) -> dict:
    lowest_replacements = min(field_by_replacement_count.values())
    replacements = {
        k for k, v in field_by_replacement_count.items() if v == lowest_replacements
    }

    for attr_name in replacements:
        attr_value = model[attr_name]
        model[attr_name] = _try_format_values(attr_value, model)

    return model


def resolve_values(model: dict, values: dict):

    # pre-fill all of the values that we can
    shortened_model = model.copy()
    for k, v in values.items():
        model[k] = v
        del shortened_model[k]
        if not shortened_model:
            return model

    fields_by_replacements_count = _create_key_to_field_count(shortened_model)
    model = _replace_placeholders(fields_by_replacements_count, model)

    values.update({k: v for k, v in model.items() if _field_count(v) == 0})
    return resolve_values(model, values)

    # values are now updates with any model fields that have been reso


m = resolve_values(fields, values)
x = 1

from datetime import datetime
from typing import Tuple

import pytest

from dynamodb_monotable.table import Table
from .testing_models import Workorder


def test_can_get_item_by_key(create_basic_table: Tuple[Table, Workorder]):
    table, item_schema = create_basic_table

    model = table.get_model(item_schema)

    # save the data in the database then see if we can get it using dynamodb_monotable
    populated_model = model.create(
        org_id=123, workorder_id=456, date_created=datetime.utcnow()
    )
    populated_model.save()

    populated_model_2 = model.create(
        org_id=456, workorder_id=456, date_created=datetime.utcnow()
    )
    populated_model_2.save()

    populated_model_3 = model.create(
        org_id=456, workorder_id=458, date_created=datetime(2019, 1, 1)
    )
    populated_model_3.save()

    # retrieve the data from the database using boto3
    results = model.query(
        hash_key="#WORKORDER", key_condition=model.sk.begins_with("#ORG:456")
    )
    for result in results:
        x = 1
    x = 1

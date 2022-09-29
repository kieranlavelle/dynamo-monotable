from datetime import datetime
from typing import Tuple

import pytest

from dynamodb_monotable.table import Table
from .testing_models import Workorder


def test_can_query_item(create_basic_table: Tuple[Table, Workorder]):
    table, item_schema = create_basic_table

    model = table.get_model(item_schema)

    # save the data in the database then see if we can get it using dynamodb_monotable
    for wo_id in range(3):
        populated_model = model.create(
            org_id=456, workorder_id=wo_id, date_created=datetime.utcnow()
        )
        populated_model.save()

    # retrieve the data from the database using boto3
    results = model.query(
        hash_key_value="#WORKORDER", key_condition=model.sk.begins_with("#ORG:456")
    )

    if results.count != 3:
        pytest.fail("Wrong number of items found in the database for query.")

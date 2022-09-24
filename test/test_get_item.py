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

    # retrieve the data from the database using boto3
    fetched_model = model.get_by_key(
        hash_key="#WORKORDER", sort_key="#ORG:123#WORKORDER:456"
    )

    if not fetched_model:
        pytest.fail("Item not found in database")

    assert fetched_model.org_id == 123


def test_can_get_item(create_basic_table: Tuple[Table, Workorder]):
    table, item_schema = create_basic_table

    model = table.get_model(item_schema)

    # save the data in the database then see if we can get it using dynamodb_monotable
    populated_model = model.create(
        org_id=123, workorder_id=456, date_created=datetime.utcnow()
    )
    populated_model.save()

    # retrieve the data from the database using boto3
    fetched_model = model.get()

    if not fetched_model:
        pytest.fail("Item not found in database")

    assert fetched_model.org_id == 123

from datetime import datetime
from typing import Tuple

import boto3
import pytest

from dynamodb_monotable.table import Table
from .testing_models import Workorder


def test_can_put_item(create_basic_table: Tuple[Table, Workorder]):
    table, item_schema = create_basic_table

    model = table.get_model(item_schema)
    model = model.create(org_id=123, workorder_id=456, date_created=datetime.utcnow())

    # persist the data into the database
    model.save()

    # retrieve the data from the database using boto3
    client = boto3.client("dynamodb")
    response = client.get_item(
        TableName=table.name,
        Key={
            "hk": {"S": "#WORKORDER"},
            "sk": {"S": "#ORG:123#WORKORDER:456"},
        },
    )

    if not response["Item"]:
        pytest.fail("Item not found in database")

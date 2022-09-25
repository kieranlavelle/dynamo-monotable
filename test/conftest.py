from typing import Tuple

import boto3
import pytest
from moto import mock_dynamodb

from dynamodb_monotable.table import Table, TableSchema
from .testing_models import Workorder


@pytest.fixture(autouse=True)
def mock_db():
    with mock_dynamodb():
        yield


@pytest.fixture
def table_name() -> str:
    return "test"


@pytest.fixture
def create_basic_table(table_name: str) -> Tuple[Table, Workorder]:
    # create a basic table using boto3
    client = boto3.client("dynamodb")
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "hk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "hk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    table = Table(
        name=table_name,
        schema=TableSchema(
            **{
                "indexes": {
                    "primary": {
                        "hash_key": {"name": "hk", "type": "S"},
                        "sort_key": {"name": "sk", "type": "S"},
                        "index_type": "primary",
                    },
                    "workorder_by_task": {
                        "hash_key": {"name": "hk", "type": "S"},
                        "sort_key": {"name": "task_id", "type": "S"},
                        "index_type": "gsi",
                        "example": "hk=#WORKORDER, task_id=#TASK:123",
                    },
                },
                "models": [Workorder],
            }
        ),
    )

    return table, Workorder

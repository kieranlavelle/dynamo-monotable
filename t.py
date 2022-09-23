from dataclasses import dataclass
from datetime import datetime


from dynamodb_monotable.table.table import Table, TableSchema
from dynamodb_monotable.table.attributes.attributes import (
    StringAttribute,
    IntAttribute,
    UTCDateTimeAttribute,
)
from dynamodb_monotable.table.models import Item


@dataclass
class Workorder(Item):
    hk: StringAttribute = StringAttribute(value="#WORKORDER")
    sk: StringAttribute = StringAttribute(
        value="#ORG:${org_id}#WORKORDER:${workorder_id}"
    )
    org_id: IntAttribute = IntAttribute(required=True)
    workorder_id: IntAttribute = IntAttribute(required=True)
    date_created: UTCDateTimeAttribute = UTCDateTimeAttribute(required=True)


schema = TableSchema(
    **{
        "indexes": {
            "primary": {
                "hash_key": {"name": "hk", "type": "S"},
                "sort_key": {"name": "sk", "type": "S"},
            }
        },
        "models": [Workorder],
    }
)

test_table = Table(
    name="test",
    schema=schema,
    client_config={
        "endpoint_url": "http://localhost:8000",
    },
)

test_table.create_table()
WorkorderItem = test_table.get_model(Workorder)
wo = WorkorderItem.create(org_id=123, workorder_id=456, date_created=datetime.utcnow())
wo.save()


def create_item(t: Table):
    try:
        t.create_table()
        WorkorderItem = t.get_model(Workorder)
        wo = WorkorderItem.create(
            org_id=123, workorder_id=456, date_created=datetime.utcnow()
        )
        wo.save()
    except Exception:
        pass
    finally:
        t.delete_table()


def get_item(t: Table):
    WorkorderItem: Workorder = t.get_model("Workorder")
    wo = WorkorderItem.get(org_id=123, workorder_id=456)
    print(wo)


if __name__ == "__main__":
    create_item(test_table)
    # x = get_item(t)

from dynamodb_monotable.table.table import Table, TableSchema
from dynamodb_monotable.table.attributes.attributes import ModelAttribute
from dynamodb_monotable.table.models import BaseItem


class Workorder(BaseItem):
    hk: ModelAttribute = ModelAttribute(value="#WORKORDER", type="S")
    sk: ModelAttribute = ModelAttribute(
        value="#ORG:${org_id}#WORKORDER:${workorder_id}", type="S"
    )
    org_id: ModelAttribute = ModelAttribute(required=True, type="N")
    workorder_id: ModelAttribute = ModelAttribute(required=True, type="N")


schema = TableSchema(
    **{
        "indexes": {
            "primary": {
                "hash_key": {"name": "hk", "type": "S"},
                "sort_key": {"name": "sk", "type": "S"},
            }
        },
        "models": {"Workorder": Workorder},
    }
)

t = Table("test", schema)
# t.create_table(endpoint_url="http://localhost:8000")

WorkorderItem = t.get_model("Workorder")
wo = WorkorderItem.create(org_id=123, workorder_id=456)
wo.save()
x = 1

from dataclasses import dataclass

from dynamodb_monotable.models import Item
from dynamodb_monotable.attributes import (
    StringAttribute,
    IntAttribute,
    UTCDateTimeAttribute,
)


@dataclass
class Workorder(Item):
    hk: StringAttribute = StringAttribute(value="#WORKORDER")
    sk: StringAttribute = StringAttribute(
        value="#ORG:${org_id}#WORKORDER:${workorder_id}"
    )
    org_id: IntAttribute = IntAttribute(required=True)
    workorder_id: IntAttribute = IntAttribute(required=True)
    date_created: UTCDateTimeAttribute = UTCDateTimeAttribute(required=True)

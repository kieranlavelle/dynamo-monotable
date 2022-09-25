from enum import Enum
from typing import Optional, Any

from pydantic import BaseModel, root_validator, Field


class IndexType(str, Enum):
    GSI = "gsi"
    LSI = "lsi"
    PRIMARY = "primary"


class Key(BaseModel):
    name: str
    type_: str = Field(..., alias="type")


class Index(BaseModel):
    hash_key: Optional[Key]
    sort_key: Optional[Key]
    index_type: IndexType = IndexType.GSI

    # this field is ignored when creating the table
    # it's only to help the user docuemnt the index
    example: Any

    @root_validator()
    def check_no_hash_key_for_lsi(cls, values: dict):
        if values["index_type"] == IndexType.LSI and values["hash_key"]:
            raise ValueError("LSI must not have a hash key")
        return values

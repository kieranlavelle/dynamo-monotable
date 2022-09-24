from enum import Enum
from typing import Optional, Dict

from pydantic import BaseModel, root_validator, Field


class IndexType(str, Enum):
    GSI = "GSI"
    LSI = "LSI"


class Key(BaseModel):
    name: str
    type_: str = Field(..., alias="type")


class PrimaryIndex(BaseModel):
    hash_key: Key
    sort_key: Optional[Key]


class AdditionalIndex(BaseModel):
    hash_key: Optional[Key]
    sort_key: Optional[Key]
    index_type: IndexType = IndexType.GSI

    @root_validator()
    def check_no_hash_key_for_lsi(cls, values: dict):
        if values["index_type"] == IndexType.LSI and values["hash_key"]:
            raise ValueError("LSI must not have a hash key")
        return values


class Indexs(BaseModel):
    primary: PrimaryIndex
    additional: Optional[Dict[str, AdditionalIndex]]

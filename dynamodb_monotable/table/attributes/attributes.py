from typing import Optional, Any

from pydantic import BaseModel, Field


class ModelAttribute(BaseModel):
    value: Optional[str]
    type_: str = Field(..., alias="type")

    # optional fields.
    required: bool = False
    default: Optional[Any] = None

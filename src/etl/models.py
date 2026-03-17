from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from typing import Optional

class User(BaseModel):
    """
    Canonical user model shared across all three pipelines.
    All transformations are applied here via vaildators
    """
    user_id: int
    email: str
    first_name: str
    last_name: str
    avatar: Optional[str] = None

    # --- Transfomation validators ---

    @field_validator('email','first_name','last_name',mode='before')
    @classmethod
    def trim_whitespace(cls, v: object) -> object:
        """Trim leading/trailing whitespace from all string fields."""
        if isinstance(v, str):
            return v.strip()
        return v
    
    @field_validator('email', mode='after')
    @classmethod
    def lowercase_email(cls, v: str) -> str:
        return v.lower()
    
    @field_validator('first_name','last_name',mode='after')
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("name field must not be empty after trimming")
        return v
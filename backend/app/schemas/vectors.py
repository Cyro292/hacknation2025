from pydantic import BaseModel
from typing import List

class Vector(BaseModel):
    """Schema for creating a vector"""
    vector: List[float]

class VectorResponse(BaseModel):
    """Schema for a vector response"""
    vector: List[float]
    
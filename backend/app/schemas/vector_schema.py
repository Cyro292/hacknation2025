from pydantic import BaseModel
from typing import List
from datetime import datetime

class Vector(BaseModel):
    """Schema for a vector"""
    vector: List[float]

class Dataset(BaseModel):
    """Schema for creating a vector"""
    id: int
    vector: Vector
    name: str
    description: str
    url: str
    created_at: datetime
    updated_at: datetime

class VectorResponse(BaseModel):
    """Schema for a vector response"""
    vector: Vector
    dataset: Dataset

class DatasetResponse(BaseModel):
    """Schema for a dataset response"""
    dataset: Dataset

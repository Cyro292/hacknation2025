from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.schemas.vector_schema import Vector, VectorResponse, DatasetResponse
from app.services.vector_service import VectorService

router = APIRouter()

@router.post("/retrieve-vector", response_model=DatasetResponse)
async def retrieve_dataset_from_vector(
    vector: Vector,
):
    """Retrieve a specific vector by ID"""
    vector = VectorService.retrieve_vector(vector=vector)
    if vector is None:
        raise HTTPException(status_code=404, detail="Vector not found")
    dataset = VectorService.retrieve_dataset(id=vector.dataset.id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse(vector=vector)

@router.post("/retrive-user", response_model=DatasetResponse)
async def retrieve_dataset_from_prompt(
    prompt: str,
):
    """Retrieve a specific user by ID"""
    vector = VectorService.retrieve_closest_vector_from_prompt(prompt=prompt)
    if vector is None:
        raise HTTPException(status_code=404, detail="Vector not found")
    dataset = VectorService.retrieve_dataset(id=vector.dataset.id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse(dataset=dataset)


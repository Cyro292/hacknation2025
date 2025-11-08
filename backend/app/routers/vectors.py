from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from app.schemas.vectors import Vector, VectorResponse
from app.services.vector_service import VectorService

router = APIRouter()

@router.post("/retrieve-vector", response_model=VectorResponse)
async def retrieve_vector(
    vector: Vector,
):
    """Retrieve a specific vector by ID"""
    vector = VectorService.retrieve_vector(vector=vector)
    if vector is None:
        raise HTTPException(status_code=404, detail="Vector not found")
    return VectorResponse(vector=vector)

@router.post("/retrive-user", response_model=VectorResponse)
async def retrieve_user(
    prompt: str,
):
    """Retrieve a specific user by ID"""
    vector = VectorService.retrieve_closest_vector_from_prompt(prompt=prompt)
    if vector is None:
        raise HTTPException(status_code=404, detail="Vector not found")
    return VectorResponse(vector=vector.vector)

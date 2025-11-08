from fastapi import APIRouter
from app.routers import vectors

api_router = APIRouter()

api_router.include_router(vectors.router, prefix="/vectors", tags=["vectors"])


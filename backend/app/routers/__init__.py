from fastapi import APIRouter
from app.routers import vector_routs

api_router = APIRouter()

api_router.include_router(vector_routs.router, prefix="/vectors", tags=["vectors"])


from app.schemas.vectors import Vector
from app.core.config import settings
# from openai import OpenAI


class VectorService:
    """Service layer for Vector operations"""

    @staticmethod
    def retrieve_closest_vector(vector: Vector) -> Vector:
        """Retrieve the closest vector to a given vector"""
#        closest_vector = openai.Embedding.search(
#            api_key=settings.OPENAI_API_KEY,
#            input=vector,
#            model="text-embedding-3-small"
#        )
#        return closest_vector
        return None

    @staticmethod
    def retrieve_closest_vector_from_prompt(prompt: str) -> Vector:
        """Create a vector from a prompt"""
        vector = VectorService.create_vector(prompt=prompt)
        a = VectorService.retrieve_closest_vector(vector=vector)
        return a

    @staticmethod
    def create_vector(prompt: str) -> Vector:
        """Create a vector from a prompt"""
#        vector = openai.Embedding.create(
#            api_key=settings.OPENAI_API_KEY,
#            input=prompt,
#            model="text-embedding-3-small"
#        )
#        return vector
        return None

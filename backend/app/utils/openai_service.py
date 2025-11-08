"""
OpenAI Service using LangChain
Provides embedding generation and structured output capabilities
"""
from typing import List, Optional, Type, TypeVar, Dict, Any
from pydantic import BaseModel
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from app.core.config import settings
from app.schemas.schemas import Dataset

T = TypeVar('T', bound=BaseModel)


class OpenAIHelper:
    """
    Service layer for OpenAI operations using LangChain.
    Provides text embeddings, dataset embeddings, and structured output.
    """
    
    def __init__(
        self,
        embedding_model: str = "text-embedding-3-large",
        chat_model: str = "gpt-4o-mini",
        temperature: float = 0.7
    ):
        """
        Initialize the OpenAI helper with LangChain components.
        
        Args:
            embedding_model: OpenAI embedding model (default: text-embedding-3-large)
            chat_model: OpenAI chat model for structured output (default: gpt-4o-mini)
            temperature: Temperature for chat model (0-2, default: 0.7)
        """
        if not settings.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        # Initialize OpenAI Embeddings
        self.embeddings = OpenAIEmbeddings(
            model=embedding_model,
            api_key=settings.OPENAI_API_KEY
        )
        
        # Initialize ChatOpenAI for structured output
        self.chat_model = ChatOpenAI(
            model=chat_model,
            temperature=temperature,
            api_key=settings.OPENAI_API_KEY
        )
    
    # ==================== EMBEDDING METHODS ====================
    
    async def create_text_embedding(self, text: str) -> List[float]:
        """
        Create an embedding vector from a single text string.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of floats representing the embedding vector
            
        Example:
            >>> helper = OpenAIHelper()
            >>> embedding = await helper.create_text_embedding("Hello world")
            >>> len(embedding)
            3072  # for text-embedding-3-large
        """
        embedding = await self.embeddings.aembed_query(text)
        return embedding
    
    async def create_text_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Create embedding vectors from multiple text strings.
        
        Args:
            texts: List of input texts to embed
            
        Returns:
            List of embedding vectors
            
        Example:
            >>> helper = OpenAIHelper()
            >>> embeddings = await helper.create_text_embeddings(["Hello", "World"])
            >>> len(embeddings)
            2
        """
        embeddings = await self.embeddings.aembed_documents(texts)
        return embeddings
    
    async def create_dataset_embedding(self, dataset: Dataset) -> List[float]:
        """
        Create an embedding vector from a prediction market dataset.
        Combines question, description, and outcomes into a single text representation.
        
        Args:
            dataset: Dataset object to embed
            
        Returns:
            List of floats representing the embedding vector
            
        Example:
            >>> helper = OpenAIHelper()
            >>> dataset = Dataset(...)
            >>> embedding = await helper.create_dataset_embedding(dataset)
        """
        # Combine relevant fields into a single text representation
        text_parts = [
            f"Question: {dataset.question}",
            f"Description: {dataset.description}",
            f"Outcomes: {', '.join(dataset.outcomes)}",
        ]
        
        # Add additional context if available
        if dataset.polymarket_id:
            text_parts.append(f"Polymarket ID: {dataset.polymarket_id}")
        
        combined_text = " | ".join(text_parts)
        
        return await self.create_text_embedding(combined_text)
    
    async def create_dataset_embeddings(self, datasets: List[Dataset]) -> List[List[float]]:
        """
        Create embedding vectors from multiple datasets.
        
        Args:
            datasets: List of Dataset objects to embed
            
        Returns:
            List of embedding vectors
            
        Example:
            >>> helper = OpenAIHelper()
            >>> datasets = [dataset1, dataset2, dataset3]
            >>> embeddings = await helper.create_dataset_embeddings(datasets)
        """
        # Prepare texts from all datasets
        texts = []
        for dataset in datasets:
            text_parts = [
                f"Question: {dataset.question}",
                f"Description: {dataset.description}",
                f"Outcomes: {', '.join(dataset.outcomes)}",
            ]
            if dataset.polymarket_id:
                text_parts.append(f"Polymarket ID: {dataset.polymarket_id}")
            
            combined_text = " | ".join(text_parts)
            texts.append(combined_text)
        
        return await self.create_text_embeddings(texts)
    
    def create_text_embedding_sync(self, text: str) -> List[float]:
        """
        Synchronous version of create_text_embedding.
        
        Args:
            text: Input text to embed
            
        Returns:
            List of floats representing the embedding vector
        """
        return self.embeddings.embed_query(text)
    
    def create_text_embeddings_sync(self, texts: List[str]) -> List[List[float]]:
        """
        Synchronous version of create_text_embeddings.
        
        Args:
            texts: List of input texts to embed
            
        Returns:
            List of embedding vectors
        """
        return self.embeddings.embed_documents(texts)
    
    # ==================== STRUCTURED OUTPUT METHODS ====================
    
    async def get_structured_output(
        self,
        prompt: str,
        response_model: Type[T],
        system_message: Optional[str] = None
    ) -> T:
        """
        Get structured output from OpenAI using Pydantic models.
        Uses LangChain's with_structured_output for reliable parsing.
        
        Args:
            prompt: User prompt/question
            response_model: Pydantic model class defining the output structure
            system_message: Optional system message to guide the model
            
        Returns:
            Instance of response_model with parsed data
            
        Example:
            >>> class Person(BaseModel):
            ...     name: str
            ...     age: int
            >>> helper = OpenAIHelper()
            >>> result = await helper.get_structured_output(
            ...     "Extract info: John is 30 years old",
            ...     Person
            ... )
            >>> result.name
            'John'
        """
        # Create structured LLM
        structured_llm = self.chat_model.with_structured_output(response_model)
        
        # Build messages
        messages = []
        if system_message:
            messages.append(SystemMessage(content=system_message))
        messages.append(HumanMessage(content=prompt))
        
        # Get structured response
        response = await structured_llm.ainvoke(messages)
        return response
    
    async def get_chat_response(
        self,
        prompt: str,
        system_message: Optional[str] = None,
        chat_history: Optional[List[Dict[str, str]]] = None
    ) -> str:
        """
        Get a text response from OpenAI chat model.
        
        Args:
            prompt: User prompt/question
            system_message: Optional system message
            chat_history: Optional chat history [{"role": "user/assistant", "content": "..."}]
            
        Returns:
            Generated text response
            
        Example:
            >>> helper = OpenAIHelper()
            >>> response = await helper.get_chat_response(
            ...     "What is LangChain?",
            ...     system_message="You are a helpful assistant"
            ... )
        """
        messages = []
        
        if system_message:
            messages.append(SystemMessage(content=system_message))
        
        if chat_history:
            from langchain_core.messages import AIMessage
            for msg in chat_history:
                if msg["role"] == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    messages.append(AIMessage(content=msg["content"]))
        
        messages.append(HumanMessage(content=prompt))
        
        response = await self.chat_model.ainvoke(messages)
        return response.content
    
    # ==================== UTILITY METHODS ====================
    
    def get_embedding_dimension(self) -> int:
        """
        Get the dimension size of the embedding model.
        
        Returns:
            Dimension size (e.g., 3072 for text-embedding-3-large)
        """
        # Create a test embedding to determine dimensions
        test_embedding = self.embeddings.embed_query("test")
        return len(test_embedding)
    
    async def similarity_search(
        self,
        query_text: str,
        corpus_texts: List[str],
        top_k: int = 5
    ) -> List[tuple[str, float]]:
        """
        Find most similar texts to a query using cosine similarity.
        
        Args:
            query_text: Query text to search for
            corpus_texts: List of texts to search through
            top_k: Number of top results to return
            
        Returns:
            List of (text, similarity_score) tuples, sorted by similarity
            
        Example:
            >>> helper = OpenAIHelper()
            >>> corpus = ["Python is great", "Java is popular", "JavaScript is everywhere"]
            >>> results = await helper.similarity_search("I love Python", corpus, top_k=2)
        """
        import numpy as np
        
        # Get embeddings
        query_embedding = await self.create_text_embedding(query_text)
        corpus_embeddings = await self.create_text_embeddings(corpus_texts)
        
        # Calculate cosine similarities
        query_norm = np.linalg.norm(query_embedding)
        similarities = []
        
        for i, corpus_embedding in enumerate(corpus_embeddings):
            corpus_norm = np.linalg.norm(corpus_embedding)
            dot_product = np.dot(query_embedding, corpus_embedding)
            similarity = dot_product / (query_norm * corpus_norm)
            similarities.append((corpus_texts[i], float(similarity)))
        
        # Sort by similarity and return top_k
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]

async def similarity_search_datasets(
    self,
    query_dataset,
    corpus_datasets: List,
    top_k: int = 5
) -> List[tuple[Any, float]]:
    """
    Find most similar datasets to a query dataset using cosine similarity of their vectors.

    Args:
        query_dataset: The dataset to use as the query
        corpus_datasets: List of datasets to compare against
        top_k: Number of top results to return

    Returns:
        List of (dataset, similarity_score) tuples, sorted by similarity
    """
    import numpy as np

    # Extract vectors
    query_vec = np.array(query_dataset.vector.vector)
    query_norm = np.linalg.norm(query_vec)

    corpus_vecs = [np.array(ds.vector.vector) for ds in corpus_datasets]

    similarities = []
    for ds, corpus_vec in zip(corpus_datasets, corpus_vecs):
        corpus_norm = np.linalg.norm(corpus_vec)
        dot_product = np.dot(query_vec, corpus_vec)
        if query_norm > 0 and corpus_norm > 0:
            similarity = dot_product / (query_norm * corpus_norm)
        else:
            similarity = 0.0
        similarities.append((ds, float(similarity)))
    
    similarities.sort(key=lambda x: x[1], reverse=True)
    return similarities[:top_k]


# ==================== SINGLETON INSTANCE ====================

_openai_helper: Optional[OpenAIHelper] = None


def get_openai_helper() -> OpenAIHelper:
    """
    Get or create the OpenAI helper singleton instance.
    
    Returns:
        OpenAIHelper instance
        
    Example:
        >>> helper = get_openai_helper()
        >>> embedding = await helper.create_text_embedding("Hello")
    """
    global _openai_helper
    if _openai_helper is None:
        _openai_helper = OpenAIHelper()
    return _openai_helper

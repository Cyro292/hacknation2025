"""
OpenAI Service using LangChain
Provides embedding generation and structured output capabilities
Now using Google Gemini 2.5 Flash for chat (faster & cheaper!)
"""
from typing import List, Optional, Type, TypeVar, Dict, Any
from pydantic import BaseModel
from langchain_openai import OpenAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage
from app.core.config import settings
from app.schemas.vector_schema import Dataset, Vector, MarketTopics, Topic
import re
import logging
import asyncio

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


class OpenAIHelper:
    """
    Service layer for OpenAI operations using LangChain.
    Provides text embeddings, dataset embeddings, and structured output.
    """
    
    def __init__(
        self,
        embedding_model: str = "text-embedding-3-large",
        chat_model: str = "gemini-2.0-flash",
        temperature: float = 0.7
    ):
        """
        Initialize the OpenAI helper with LangChain components.
        Uses OpenAI for embeddings and Google Gemini for chat (faster & cheaper).
        
        Args:
            embedding_model: OpenAI embedding model (default: text-embedding-3-large)
            chat_model: Google Gemini model for structured output (default: gemini-2.0-flash-exp)
            temperature: Temperature for chat model (0-2, default: 0.7)
        """
        if not settings.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        if not settings.GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY not found in environment variables")
        
        # Initialize OpenAI Embeddings (keeping OpenAI for high-quality embeddings)
        self.embeddings = OpenAIEmbeddings(
            model=embedding_model,
            api_key=settings.OPENAI_API_KEY
        )
        
        # Initialize Google Gemini for chat (faster & cheaper!)
        self.chat_model = ChatGoogleGenerativeAI(
            model=chat_model,
            temperature=temperature,
            google_api_key=settings.GOOGLE_API_KEY
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
        Optimized with parallel processing for better performance.
        
        Args:
            datasets: List of Dataset objects to embed
            
        Returns:
            List of embedding vectors
            
        Example:
            >>> helper = OpenAIHelper()
            >>> datasets = [dataset1, dataset2, dataset3]
            >>> embeddings = await helper.create_dataset_embeddings(datasets)
        """
        # Prepare texts from all datasets in parallel
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
        
        # Batch process with OpenAI API (already optimized internally)
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
    
    # ==================== TOPIC GENERATION ====================
    
    async def generate_market_topics(
        self,
        question: str,
        description: Optional[str] = None,
        outcomes: Optional[List[str]] = None
    ) -> List[Topic]:
        """
        Generate ~15 logical topics for a market using AI.
        Topics capture the logical/conceptual meaning rather than grammatical structure.
        
        Args:
            question: Market question
            description: Optional market description
            outcomes: Optional list of outcomes
            
        Returns:
            List of Topic objects with name and description
        """
        # Build market context
        market_context = f"Market Question: {question}\n"
        if description:
            market_context += f"Description: {description}\n"
        if outcomes:
            market_context += f"Outcomes: {', '.join(outcomes)}\n"
        
        system_message = """You are an expert at analyzing prediction markets and extracting logical, conceptual topics.

Your task is to identify approximately 15 key topics that capture the LOGICAL and CONCEPTUAL meaning of the market, 
not just grammatical or surface-level features.

Each topic should be:
- A specific, well-defined concept (e.g., "embedded banking APIs for banks", "fintech regulation in Europe", "startup product-market fit for B2B SaaS")
- Described clearly to explain what the concept means
- Focused on the underlying themes, domains, industries, technologies, or concepts relevant to the market
- Logically related to the market's subject matter

Examples of good topics:
- "embedded banking APIs for banks" - description: "APIs that allow banks to integrate banking services into third-party applications"
- "fintech regulation in Europe" - description: "Regulatory frameworks governing financial technology companies in European markets"
- "startup product-market fit for B2B SaaS" - description: "The alignment between a B2B SaaS product and its target market's needs"

Generate topics that would help someone understand what this market is really about at a conceptual level."""

        prompt = f"""Analyze the following prediction market and generate approximately 15 logical topics that capture its conceptual meaning:

{market_context}

Generate topics that represent the key concepts, domains, industries, technologies, or themes that this market relates to."""

        try:
            result = await self.get_structured_output(
                prompt=prompt,
                response_model=MarketTopics,
                system_message=system_message
            )
            return result.topics
        except Exception as e:
            logger.error(f"Error generating topics: {e}")
            # Fallback: return empty list or basic topics
            return []
    
    # ==================== QUERY PREPROCESSING ====================
    
    def preprocess_query(self, query: str) -> str:
        """
        Preprocess a user query before creating embeddings.
        Cleans, lowercases, and removes extraneous punctuation.
        
        Args:
            query: Raw user query
            
        Returns:
            Cleaned and preprocessed query string
        """
        # Lowercase
        query = query.lower()
        
        # Remove extraneous punctuation (keep basic sentence punctuation)
        # Keep: letters, numbers, spaces, basic punctuation (. , ? !)
        query = re.sub(r'[^\w\s.,?!-]', ' ', query)
        
        # Remove multiple spaces
        query = re.sub(r'\s+', ' ', query)
        
        # Strip leading/trailing whitespace
        query = query.strip()
        
        return query
    
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
        Optimized with parallel embedding generation for better performance.
        
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
        
        # Get embeddings in parallel for maximum speed
        query_embedding, corpus_embeddings = await asyncio.gather(
            self.create_text_embedding(query_text),
            self.create_text_embeddings(corpus_texts)
        )
        
        # Calculate cosine similarities using vectorized operations
        query_vec = np.array(query_embedding)
        corpus_vecs = np.array(corpus_embeddings)
        
        # Vectorized cosine similarity calculation (much faster!)
        query_norm = np.linalg.norm(query_vec)
        corpus_norms = np.linalg.norm(corpus_vecs, axis=1)
        dot_products = np.dot(corpus_vecs, query_vec)
        similarities = dot_products / (query_norm * corpus_norms)
        
        # Create result tuples
        results = [(corpus_texts[i], float(similarities[i])) for i in range(len(corpus_texts))]
        
        # Sort by similarity and return top_k
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    async def similarity_search_datasets(
        self,
        query_dataset,
        corpus_datasets: List,
        top_k: int = 5
    ) -> List[tuple[Any, float]]:
        """
        Find most similar datasets to a query dataset using cosine similarity of their vectors.
        Optimized with vectorized operations for better performance.

        Args:
            query_dataset: The dataset to use as the query
            corpus_datasets: List of datasets to compare against
            top_k: Number of top results to return

        Returns:
            List of (dataset, similarity_score) tuples, sorted by similarity
        """
        import numpy as np

        # Extract vectors using vectorized operations
        query_vec = np.array(query_dataset.vector.vector)
        query_norm = np.linalg.norm(query_vec)

        corpus_vecs = np.array([np.array(ds.vector.vector) for ds in corpus_datasets])
        
        # Vectorized cosine similarity calculation (much faster!)
        corpus_norms = np.linalg.norm(corpus_vecs, axis=1)
        dot_products = np.dot(corpus_vecs, query_vec)
        
        # Handle zero norms
        with np.errstate(divide='ignore', invalid='ignore'):
            similarities = dot_products / (query_norm * corpus_norms)
            similarities = np.nan_to_num(similarities, nan=0.0)
        
        # Create result tuples
        results = [(corpus_datasets[i], float(similarities[i])) for i in range(len(corpus_datasets))]
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    # ==================== BATCH PROCESSING METHODS ====================
    
    async def batch_generate_topics(
        self,
        markets: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> List[List[Topic]]:
        """
        Generate topics for multiple markets concurrently with controlled parallelism.
        
        Args:
            markets: List of market dictionaries with 'question', 'description', 'outcomes'
            max_concurrent: Maximum number of concurrent API calls (default: 5)
            
        Returns:
            List of topic lists, one per market
            
        Example:
            >>> helper = OpenAIHelper()
            >>> markets = [
            ...     {"question": "Will...", "description": "...", "outcomes": [...]},
            ...     {"question": "Will...", "description": "...", "outcomes": [...]}
            ... ]
            >>> all_topics = await helper.batch_generate_topics(markets)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def generate_with_semaphore(market):
            async with semaphore:
                try:
                    return await self.generate_market_topics(
                        question=market.get("question", ""),
                        description=market.get("description"),
                        outcomes=market.get("outcomes")
                    )
                except Exception as e:
                    logger.error(f"Error generating topics for market: {e}")
                    return []
        
        # Process all markets concurrently with rate limiting
        return await asyncio.gather(*[generate_with_semaphore(m) for m in markets])
    
    async def batch_embeddings_with_limit(
        self,
        texts: List[str],
        batch_size: int = 100
    ) -> List[List[float]]:
        """
        Create embeddings for large lists of texts with automatic batching.
        Useful for processing thousands of texts efficiently.
        
        Args:
            texts: List of texts to embed
            batch_size: Number of texts per batch (default: 100)
            
        Returns:
            List of embedding vectors
            
        Example:
            >>> helper = OpenAIHelper()
            >>> texts = ["text1", "text2", ..., "text10000"]
            >>> embeddings = await helper.batch_embeddings_with_limit(texts)
        """
        if not texts:
            return []
        
        # Split into batches
        batches = [texts[i:i + batch_size] for i in range(0, len(texts), batch_size)]
        
        # Process batches in parallel
        batch_results = await asyncio.gather(*[
            self.create_text_embeddings(batch) for batch in batches
        ])
        
        # Flatten results
        return [emb for batch_result in batch_results for emb in batch_result]


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

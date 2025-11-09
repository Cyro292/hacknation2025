"""
Vector Service - Handles vector embeddings stored in database
"""
from typing import List, Optional, Tuple
from app.schemas.vector_schema import VectorEmbedding, Dataset
from app.core.config import settings
from app.utils.openai_service import get_openai_helper
from app.services.database_service import get_database_service
import logging
import numpy as np
import asyncio
import time
from collections import deque

logger = logging.getLogger(__name__)


class BurstRateLimiter:
    """
    Burst rate limiter - schedules 1000 requests at once, then waits 65s before next burst.
    Simple and efficient for high-throughput processing.
    """
    def __init__(self, burst_size: int = 1000, wait_seconds: float = 65):
        self.burst_size = burst_size
        self.wait_seconds = wait_seconds
        self.burst_start_time = None
        self.requests_in_burst = 0
        self.total_requests = 0
        self.lock = asyncio.Lock()
    
    async def start_burst(self):
        """Mark the start of a new burst."""
        async with self.lock:
            self.burst_start_time = time.time()
            self.requests_in_burst = 0
            logger.info(f"🚀 Starting new burst of up to {self.burst_size} requests...")
    
    async def wait_for_next_burst(self):
        """Wait until we can start the next burst (65s from last burst start)."""
        async with self.lock:
            if self.burst_start_time is None:
                return  # First burst, no waiting
            
            elapsed = time.time() - self.burst_start_time
            wait_time = self.wait_seconds - elapsed
            
            if wait_time > 0:
                logger.info(f"⏳ Waiting {wait_time:.1f}s before next burst (to respect 1000 RPM limit)...")
                await asyncio.sleep(wait_time)
            
            logger.info(f"✓ Ready for next burst! (Total requests so far: {self.total_requests})")
    
    def record_request(self):
        """Record a request in the current burst."""
        self.requests_in_burst += 1
        self.total_requests += 1
    
    def get_burst_count(self) -> int:
        """Get number of requests in current burst."""
        return self.requests_in_burst


class VectorService:
    """Service for vector operations using stored embeddings."""
    
    def __init__(self):
        self._openai_helper = None
        self.db_service = get_database_service()
        self.rate_limiter = BurstRateLimiter(burst_size=1000, wait_seconds=65)  # 1000 RPM limit
    
    @property
    def openai_helper(self):
        """Lazy initialization of OpenAI helper - only when needed."""
        if self._openai_helper is None:
            self._openai_helper = get_openai_helper()
        return self._openai_helper
    
    # ==================== CREATE & STORE EMBEDDINGS ====================
    
    async def create_and_store_embedding(self, market_id: int) -> VectorEmbedding:
        """Create embedding for a market using AI-generated topics and store it in database."""
        try:
            # Get market
            market = await self.db_service.get_market_by_id(market_id)
            if not market:
                raise ValueError(f"Market {market_id} not found")
            
            # Generate topics using AI
            logger.info(f"Generating topics for market {market_id}...")
            topics = await self.openai_helper.generate_market_topics(
                question=market.question,
                description=market.description,
                outcomes=market.outcomes if market.outcomes else None
            )
            
            if not topics:
                logger.warning(f"No topics generated for market {market_id}, using fallback")
                # Fallback: use original text if topic generation fails
                text_parts = [f"Question: {market.question}"]
                if market.description:
                    text_parts.append(f"Description: {market.description}")
                text = " | ".join(text_parts)
            else:
                # Create text representation from topics
                # Format: "Topic1: Description1 | Topic2: Description2 | ..."
                topic_texts = [f"{topic.name}: {topic.description}" for topic in topics]
                text = " | ".join(topic_texts)
                logger.info(f"Generated {len(topics)} topics for market {market_id}")
            
            # Create embedding from topics text
            embedding = await self.openai_helper.create_text_embedding(text)
            
            # Convert topics to dict format for storage
            topics_dict = [{"name": topic.name, "description": topic.description} for topic in topics] if topics else None
            
            # Store in database with topics
            return await self.db_service.store_embedding(market_id, embedding, topics=topics_dict)
            
        except Exception as e:
            logger.error(f"Error creating embedding for market {market_id}: {e}")
            raise
    
    async def batch_create_embeddings(self, market_ids: List[int], batch_size: int = 1000) -> dict:
        """
        Create embeddings for multiple markets using batch API calls.
        Processes in bursts of 1000 requests with 65s wait between bursts.
        
        Returns:
            Dict with created/failed counts
        """
        import time
        created = 0
        failed = 0
        
        # Process in bursts of 1000 (respecting RPM limit)
        for i in range(0, len(market_ids), batch_size):
            batch_start = time.time()
            batch_ids = market_ids[i:i+batch_size]
            
            # Wait for rate limit window if needed
            if i > 0:  # Not the first batch
                await self.rate_limiter.wait_for_next_burst()
            
            try:
                # Get all markets in batch with ONE query (much faster!)
                fetch_start = time.time()
                markets = await self.db_service.batch_get_markets_by_ids(batch_ids)
                fetch_time = time.time() - fetch_start
                logger.info(f"  ⚡ Fetched {len(markets)} markets in {fetch_time:.2f}s")
                
                if not markets:
                    continue
                
                # Start new burst
                await self.rate_limiter.start_burst()
                
                # Generate topics CONCURRENTLY for all markets (MUCH FASTER!)
                topics_start = time.time()
                logger.info(f"  🧠 Generating topics for {len(markets)} markets (ALL AT ONCE - burst mode!)...")
                
                # Async function to generate topics for one market
                # No semaphore - fire ALL at once!
                
                async def generate_topics_for_market(market, idx):
                    market_start = time.time()
                    try:
                        # Record this request in the burst
                        self.rate_limiter.record_request()
                        
                        topics = await self.openai_helper.generate_market_topics(
                            question=market.question,
                            description=market.description,
                            outcomes=market.outcomes if market.outcomes else None
                        )
                        
                        market_time = time.time() - market_start
                        burst_count = self.rate_limiter.get_burst_count()
                        
                        if not topics:
                            logger.warning(f"    [{idx+1}/{len(markets)}] Market {market.id}: No topics (fallback) - {market_time:.2f}s | Burst: {burst_count}")
                            text_parts = [f"Question: {market.question}"]
                            if market.description:
                                text_parts.append(f"Description: {market.description}")
                            if market.outcomes:
                                text_parts.append(f"Outcomes: {', '.join(market.outcomes)}")
                            text = " | ".join(text_parts)
                            return (market.id, text, None)
                        else:
                            topic_texts = [f"{topic.name}: {topic.description}" for topic in topics]
                            text = " | ".join(topic_texts)
                            topics_list = [{"name": topic.name, "description": topic.description} for topic in topics]
                            logger.info(f"    [{idx+1}/{len(markets)}] Market {market.id}: {len(topics)} topics - {market_time:.2f}s | Burst: {burst_count}")
                            return (market.id, text, topics_list)
                            
                    except Exception as e:
                        market_time = time.time() - market_start
                        logger.error(f"    [{idx+1}/{len(markets)}] Market {market.id}: ERROR - {market_time:.2f}s - {e}")
                        text_parts = [f"Question: {market.question}"]
                        if market.description:
                            text_parts.append(f"Description: {market.description}")
                        if market.outcomes:
                            text_parts.append(f"Outcomes: {', '.join(market.outcomes)}")
                        text = " | ".join(text_parts)
                        return (market.id, text, None)
                
                # Generate all topics concurrently with rate limiting!
                results = await asyncio.gather(*[
                    generate_topics_for_market(market, idx) 
                    for idx, market in enumerate(markets)
                ])
                
                # Prepare texts and maps from results
                texts = []
                market_map = {}
                topics_map = {}
                
                for idx, (market_id, text, topics) in enumerate(results):
                    texts.append(text)
                    market_map[idx] = market_id
                    topics_map[market_id] = topics
                
                topics_time = time.time() - topics_start
                burst_count = self.rate_limiter.get_burst_count()
                logger.info(f"  ✓ Topic generation complete: {topics_time:.2f}s total ({topics_time/len(markets):.2f}s avg per market)")
                logger.info(f"  📊 Burst stats: {burst_count} requests fired in this burst")
                
                # Batch create embeddings via OpenAI
                # Split into smaller chunks to avoid OpenAI's 300k token limit
                embed_start = time.time()
                embeddings = []
                embedding_chunk_size = 100  # Process 100 embeddings at a time (safe limit)
                
                logger.info(f"  🔢 Creating embeddings in chunks of {embedding_chunk_size}...")
                for chunk_i in range(0, len(texts), embedding_chunk_size):
                    chunk_texts = texts[chunk_i:chunk_i+embedding_chunk_size]
                    try:
                        chunk_embeddings = await self.openai_helper.create_text_embeddings(chunk_texts)
                        embeddings.extend(chunk_embeddings)
                        logger.info(f"     Created {len(chunk_embeddings)} embeddings (chunk {chunk_i//embedding_chunk_size + 1}/{(len(texts) + embedding_chunk_size - 1)//embedding_chunk_size})")
                    except Exception as e:
                        logger.error(f"     Error creating embeddings for chunk {chunk_i//embedding_chunk_size + 1}: {e}")
                        # Create zero embeddings for failed chunk to maintain alignment
                        for _ in range(len(chunk_texts)):
                            embeddings.append([0.0] * 3072)  # Fallback zero embedding
                
                embed_time = time.time() - embed_start
                logger.info(f"  ✓ Created {len(embeddings)} embeddings in {embed_time:.2f}s")
                
                # Prepare batch data for Supabase
                embeddings_to_store = []
                for idx, embedding in enumerate(embeddings):
                    market_id = market_map[idx]
                    embeddings_to_store.append({
                        'market_id': market_id,
                        'embedding': embedding,
                        'topics': topics_map.get(market_id)
                    })
                
                # Batch store to Supabase (much faster!)
                store_start = time.time()
                try:
                    result = await self.db_service.batch_store_embeddings(embeddings_to_store, batch_size=50)
                    created += result['successful']
                    failed += result['failed']
                    store_time = time.time() - store_start
                    logger.info(f"  💾 Stored {result['successful']} embeddings in {store_time:.2f}s")
                except Exception as e:
                    store_time = time.time() - store_start
                    logger.error(f"  ✗ Batch storage failed in {store_time:.2f}s: {e}")
                    failed += len(embeddings_to_store)
                
                batch_time = time.time() - batch_start
                logger.info(f"  ⏱️  Batch complete: {batch_time:.2f}s total")
                
            except Exception as e:
                batch_time = time.time() - batch_start
                logger.error(f"  ✗ Batch processing failed after {batch_time:.2f}s for batch {i}-{i+batch_size}: {e}")
                failed += len(batch_ids)
        
        return {"created": created, "failed": failed, "total": len(market_ids)}
    
    # ==================== SIMILARITY SEARCH ====================
    
    async def find_similar_markets(
        self,
        query_embedding: List[float],
        limit: int = 10
    ) -> List[Tuple[int, float]]:
        """
        Find similar markets using stored embeddings.
        Returns list of (market_id, similarity_score).
        """
        try:
            # Get all stored embeddings
            all_embeddings = await self.db_service.get_all_embeddings()
            
            if not all_embeddings:
                return []
            
            # Calculate similarities
            query_array = np.array(query_embedding)
            query_norm = np.linalg.norm(query_array)
            
            similarities = []
            for emb in all_embeddings:
                emb_array = np.array(emb.embedding)
                emb_norm = np.linalg.norm(emb_array)
                
                if emb_norm == 0:
                    continue
                
                # Cosine similarity
                dot_product = np.dot(query_array, emb_array)
                similarity = float(dot_product / (query_norm * emb_norm))
                similarities.append((emb.market_id, similarity))
            
            # Sort and return top results
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:limit]
            
        except Exception as e:
            logger.error(f"Error in similarity search: {e}")
            raise
    
    async def find_similar_to_market(
        self,
        market_id: int,
        limit: int = 10
    ) -> List[Tuple[int, float]]:
        """Find markets similar to a given market using stored embeddings."""
        try:
            # Get embedding for the market
            embedding = await self.db_service.get_embedding(market_id)
            if not embedding:
                raise ValueError(f"No embedding found for market {market_id}")
            
            # Find similar
            results = await self.find_similar_markets(embedding.embedding, limit=limit + 1)
            
            # Filter out the query market itself
            return [(mid, score) for mid, score in results if mid != market_id][:limit]
            
        except Exception as e:
            logger.error(f"Error finding similar markets: {e}")
            raise
    
    async def find_similar_to_text(
        self,
        query_text: str,
        limit: int = 10
    ) -> List[Tuple[int, float]]:
        """Find markets similar to a text query."""
        try:
            # Preprocess query (cleaning, lowercasing, removing punctuation)
            cleaned_query = self.openai_helper.preprocess_query(query_text)
            
            # Create embedding for preprocessed query
            query_embedding = await self.openai_helper.create_text_embedding(cleaned_query)
            
            # Find similar
            return await self.find_similar_markets(query_embedding, limit=limit)
            
        except Exception as e:
            logger.error(f"Error finding similar markets to text: {e}")
            raise
    
    # ==================== PROXIMITY SEARCH ====================
    
    async def find_markets_in_proximity(
        self,
        query_embedding: List[float],
        threshold: float = 0.7
    ) -> List[Tuple[int, float]]:
        """
        Find all markets within a certain proximity (similarity threshold).
        Returns all markets with similarity >= threshold.
        
        Args:
            query_embedding: Query vector embedding
            threshold: Minimum similarity score (0.0 to 1.0, default: 0.7)
        
        Returns:
            List of (market_id, similarity_score) tuples above threshold
        """
        try:
            # Get all stored embeddings
            all_embeddings = await self.db_service.get_all_embeddings()
            
            if not all_embeddings:
                return []
            
            # Calculate similarities
            query_array = np.array(query_embedding)
            query_norm = np.linalg.norm(query_array)
            
            results = []
            for emb in all_embeddings:
                emb_array = np.array(emb.embedding)
                emb_norm = np.linalg.norm(emb_array)
                
                if emb_norm == 0:
                    continue
                
                # Cosine similarity
                dot_product = np.dot(query_array, emb_array)
                similarity = float(dot_product / (query_norm * emb_norm))
                
                # Only include if above threshold
                if similarity >= threshold:
                    results.append((emb.market_id, similarity))
            
            # Sort by similarity (highest first)
            results.sort(key=lambda x: x[1], reverse=True)
            return results
            
        except Exception as e:
            logger.error(f"Error in proximity search: {e}")
            raise
    
    async def find_markets_in_proximity_to_market(
        self,
        market_id: int,
        threshold: float = 0.7
    ) -> List[Tuple[int, float]]:
        """
        Find all markets within proximity to a given market.
        
        Args:
            market_id: Reference market ID
            threshold: Minimum similarity score (0.0 to 1.0, default: 0.7)
        
        Returns:
            List of (market_id, similarity_score) tuples (excluding the query market)
        """
        try:
            # Get embedding for the market
            embedding = await self.db_service.get_embedding(market_id)
            if not embedding:
                raise ValueError(f"No embedding found for market {market_id}")
            
            # Find all in proximity
            results = await self.find_markets_in_proximity(embedding.embedding, threshold=threshold)
            
            # Filter out the query market itself
            return [(mid, score) for mid, score in results if mid != market_id]
            
        except Exception as e:
            logger.error(f"Error finding markets in proximity: {e}")
            raise
    
    async def find_markets_in_proximity_to_text(
        self,
        query_text: str,
        threshold: float = 0.7
    ) -> List[Tuple[int, float]]:
        """
        Find all markets within proximity to a text query.
        
        Args:
            query_text: Text query
            threshold: Minimum similarity score (0.0 to 1.0, default: 0.7)
        
        Returns:
            List of (market_id, similarity_score) tuples
        """
        try:
            # Preprocess query (cleaning, lowercasing, removing punctuation)
            cleaned_query = self.openai_helper.preprocess_query(query_text)
            
            # Create embedding for preprocessed query
            query_embedding = await self.openai_helper.create_text_embedding(cleaned_query)
            
            # Find all in proximity
            return await self.find_markets_in_proximity(query_embedding, threshold=threshold)
            
        except Exception as e:
            logger.error(f"Error finding markets in proximity to text: {e}")
            raise


# ==================== SINGLETON ====================

_vector_service: Optional[VectorService] = None


def get_vector_service() -> VectorService:
    global _vector_service
    if _vector_service is None:
        _vector_service = VectorService()
    return _vector_service

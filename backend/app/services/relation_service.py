"""
Relation Service - Manages stored market relationships in database
"""
from typing import List, Optional, Tuple
from app.schemas.relation_schema import MarketRelation, MarketRelationCreate
from app.schemas.market_schema import Market
from app.services.database_service import get_database_service
from app.services.vector_service import get_vector_service
from app.utils.market_analysis import analyze_market_correlation
import logging
import numpy as np
import asyncio

logger = logging.getLogger(__name__)


class RelationService:
    """Manages stored market relationships in database."""
    
    def __init__(self):
        self.db = get_database_service()
        self._vector_service = None
    
    @property
    def vector_service(self):
        """Lazy initialization of vector service - only when needed."""
        if self._vector_service is None:
            self._vector_service = get_vector_service()
        return self._vector_service
    
    async def get_related_markets(
        self,
        market_id: int,
        limit: int = 10,
        min_similarity: float = 0.7,
        min_volume: Optional[float] = None,
        include_ai_analysis: bool = False,
        ai_model: str = "gemini-flash"
    ) -> List[Tuple[int, float, float, float, Optional[float], Optional[str], Optional[float], Optional[str], Optional[str]]]:
        """
        Get related markets from stored relations.
        
        Args:
            market_id: Source market ID
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold
            min_volume: Minimum market volume filter (optional)
            include_ai_analysis: Include AI-generated correlation analysis (default: False)
            ai_model: AI model to use for analysis ("gemini-flash" or "gemini-pro")
            
        Returns:
            List of (related_market_id, similarity, correlation, pressure, ai_score, ai_explanation, 
                     investment_score, investment_rationale, risk_level) tuples
        """
        try:
            # Query relations where this market is involved
            response = self.db.client.table('market_relations')\
                .select('*')\
                .or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")\
                .gte('similarity', min_similarity)\
                .order('similarity', desc=True)\
                .limit(limit * 3)\
                .execute()
            
            # Extract all related market IDs
            basic_results = []
            for relation in response.data:
                related_id = (
                    relation['market_id_2'] 
                    if relation['market_id_1'] == market_id 
                    else relation['market_id_1']
                )
                basic_results.append((
                    related_id,
                    float(relation['similarity']),
                    float(relation.get('correlation', 0.0)),
                    float(relation.get('pressure', 0.0))
                ))
            
            # If no filtering or AI needed, return immediately (sorted by pressure)
            if min_volume is None and not include_ai_analysis:
                # Sort by pressure (descending) before returning
                sorted_results = sorted(basic_results[:limit], key=lambda x: -x[3])  # x[3] is pressure
                return [(mid, sim, corr, press, None, None, None, None, None) for mid, sim, corr, press in sorted_results]
            
            # Fetch ALL markets in a SINGLE batch request (MUCH FASTER!)
            market_ids_to_fetch = [mid for mid, _, _, _ in basic_results]
            markets = await self.db.batch_get_markets_by_ids(market_ids_to_fetch)
            
            # Build market cache
            market_cache = {market.id: market for market in markets}
            
            # Apply volume filter if needed
            results = []
            for related_id, similarity, correlation, pressure in basic_results:
                if min_volume is not None:
                    market = market_cache.get(related_id)
                    if not market or (market.volume or 0.0) < min_volume:
                        continue
                
                results.append((related_id, similarity, correlation, pressure))
                
                # Stop if we have enough results
                if len(results) >= limit:
                    break
            
            # If no AI analysis needed, return quickly (sorted by pressure)
            if not include_ai_analysis:
                # Sort by pressure (descending)
                results.sort(key=lambda x: -x[3])  # x[3] is pressure
                return [(mid, sim, corr, press, None, None, None, None, None) for mid, sim, corr, press in results]
            
            # AI analysis enabled - process in parallel
            logger.info(f"Performing AI analysis for {len(results)} markets in parallel...")
            
            # Get source market for AI analysis
            source_market = await self.db.get_market_by_id(market_id)
            if not source_market:
                return [(mid, sim, corr, press, None, None, None, None, None) for mid, sim, corr, press in results]
            
            async def analyze_one(related_id, similarity, correlation, pressure):
                market = market_cache.get(related_id)
                if not market:
                    market = await self.db.get_market_by_id(related_id)
                
                if not market:
                    return (related_id, similarity, correlation, pressure, None, None, None, None, None)
                
                try:
                    analysis = await analyze_market_correlation(
                        market1=source_market,
                        market2=market,
                        model=ai_model
                    )
                    return (
                        related_id, 
                        similarity, 
                        correlation, 
                        pressure, 
                        analysis.correlation_score, 
                        analysis.explanation,
                        analysis.investment_score,
                        analysis.investment_rationale,
                        analysis.risk_level
                    )
                except Exception as e:
                    logger.warning(f"Failed AI analysis for market {related_id}: {e}")
                    return (related_id, similarity, correlation, pressure, None, None, None, None, None)
            
            # Process all in parallel
            analysis_tasks = [analyze_one(mid, sim, corr, press) for mid, sim, corr, press in results]
            results_with_ai = await asyncio.gather(*analysis_tasks)
            
            # Sort by investment score (descending) then pressure (descending)
            # Investment score is at index 6, pressure at index 3
            results_with_ai.sort(
                key=lambda x: (
                    -(x[6] if x[6] is not None else -1),  # Investment score (higher first, None = -1)
                    -x[3]  # Pressure (higher first)
                ),
                reverse=False  # Because we're using negative values
            )
            
            logger.info(f"✓ Completed AI analysis for {len(results_with_ai)} markets")
            return results_with_ai
            
        except Exception as e:
            logger.error(f"Error getting related markets: {e}")
            raise
    
    async def get_related_markets_enriched(
        self,
        market_id: int,
        limit: int = 10,
        min_similarity: float = 0.7,
        min_volume: Optional[float] = None,
        include_source: bool = True,
        include_ai_analysis: bool = False,
        ai_model: str = "gemini-flash"
    ) -> dict:
        """
        Get related markets from stored relations with full market details and optional AI correlation analysis.
        
        Args:
            market_id: Source market ID
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold
            min_volume: Minimum market volume filter (optional)
            include_source: Whether to include source market details (default: True)
            include_ai_analysis: Whether to include AI-generated correlation analysis (default: False for speed)
            ai_model: AI model to use for analysis ("gemini-flash" or "gemini-pro")
            
        Returns:
            Dictionary with:
            - source_market: Market object (if include_source=True, else None)
            - related_markets: List of (related_market_id, similarity, correlation, pressure, market_object, 
                               ai_score, ai_explanation, investment_score, investment_rationale, risk_level) tuples
        """
        try:
            # Get source market if requested
            source_market = None
            if include_source:
                source_market = await self.db.get_market_by_id(market_id)
                if not source_market:
                    raise ValueError(f"Source market {market_id} not found")
            
            # Get basic relations (without AI analysis - we'll do that separately)
            basic_results = await self.get_related_markets(
                market_id=market_id,
                limit=limit,
                min_similarity=min_similarity,
                min_volume=min_volume,
                include_ai_analysis=False  # We'll handle AI separately with full market objects
            )
            
            # Fetch all market details in a SINGLE batch request (MUCH FASTER!)
            # Extract just the first 4 values (id, sim, corr, press) ignoring AI fields
            market_ids_to_fetch = [related_id for related_id, _, _, _, _, _, _, _, _ in basic_results]
            markets = await self.db.batch_get_markets_by_ids(market_ids_to_fetch)
            
            # Build market lookup
            market_lookup = {market.id: market for market in markets}
            
            # If AI analysis is NOT needed, return quickly (sorted by pressure)
            if not include_ai_analysis:
                enriched_results = []
                for related_id, similarity, correlation, pressure, _, _, _, _, _ in basic_results:
                    market = market_lookup.get(related_id)
                    if market:
                        enriched_results.append((
                            related_id,
                            similarity,
                            correlation,
                            pressure,
                            market,
                            None,  # ai_correlation_score
                            None,  # ai_explanation
                            None,  # investment_score
                            None,  # investment_rationale
                            None   # risk_level
                        ))
                
                # Sort by pressure (descending)
                enriched_results.sort(key=lambda x: -x[3])  # x[3] is pressure
                
                return {
                    "source_market": source_market,
                    "related_markets": enriched_results
                }
            
            # AI analysis enabled - process in parallel with rate limiting
            logger.info(f"Performing AI analysis for {len(basic_results)} markets in parallel...")
            
            async def analyze_one_market(related_id, similarity, correlation, pressure):
                market = market_lookup.get(related_id)
                if not market:
                    return None
                
                ai_correlation_score = None
                ai_explanation = None
                investment_score = None
                investment_rationale = None
                risk_level = None
                
                if source_market:
                    try:
                        analysis = await analyze_market_correlation(
                            market1=source_market,
                            market2=market,
                            model=ai_model
                        )
                        ai_correlation_score = analysis.correlation_score
                        ai_explanation = analysis.explanation
                        investment_score = analysis.investment_score
                        investment_rationale = analysis.investment_rationale
                        risk_level = analysis.risk_level
                    except Exception as e:
                        logger.warning(f"Failed AI analysis for market {related_id}: {e}")
                
                return (
                    related_id,
                    similarity,
                    correlation,
                    pressure,
                    market,
                    ai_correlation_score,
                    ai_explanation,
                    investment_score,
                    investment_rationale,
                    risk_level
                )
            
            # Process all AI analyses in parallel (MUCH FASTER!)
            analysis_tasks = [
                analyze_one_market(related_id, similarity, correlation, pressure)
                for related_id, similarity, correlation, pressure, _, _, _, _, _ in basic_results
            ]
            
            results_with_ai = await asyncio.gather(*analysis_tasks)
            enriched_results = [r for r in results_with_ai if r is not None]
            
            # Sort by investment score (descending) then pressure (descending)
            # Investment score is at index 7, pressure at index 3
            enriched_results.sort(
                key=lambda x: (
                    -(x[7] if x[7] is not None else -1),  # Investment score (higher first, None = -1)
                    -x[3]  # Pressure (higher first)
                ),
                reverse=False  # Because we're using negative values
            )
            
            logger.info(f"✓ Completed AI analysis for {len(enriched_results)} markets")
            
            return {
                "source_market": source_market,
                "related_markets": enriched_results
            }
            
        except Exception as e:
            logger.error(f"Error getting enriched related markets: {e}")
            raise
    
    async def get_relation_between(
        self,
        market_id_1: int,
        market_id_2: int
    ) -> Optional[MarketRelation]:
        """Get relation between two specific markets."""
        try:
            # Ensure market_id_1 < market_id_2 for query
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            response = self.db.client.table('market_relations')\
                .select('*')\
                .eq('market_id_1', min_id)\
                .eq('market_id_2', max_id)\
                .execute()
            
            if response.data:
                return MarketRelation(**response.data[0])
            return None
            
        except Exception as e:
            logger.error(f"Error getting relation between markets: {e}")
            raise
    
    async def create_relation(
        self,
        market_id_1: int,
        market_id_2: int,
        similarity: float,
        correlation: float = 0.0,
        pressure: float = 0.0
    ) -> MarketRelation:
        """
        Create or update a relation between two markets.
        
        Args:
            market_id_1: First market ID
            market_id_2: Second market ID
            similarity: Similarity score (0.0-1.0)
            correlation: Correlation score (default: 0.0)
            pressure: Pressure score (default: 0.0)
            
        Returns:
            Created MarketRelation
        """
        try:
            # Ensure market_id_1 < market_id_2
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            data = {
                'market_id_1': min_id,
                'market_id_2': max_id,
                'similarity': similarity,
                'correlation': correlation,
                'pressure': pressure
            }
            
            # Upsert: update if exists, insert if not
            response = self.db.client.table('market_relations').upsert(
                data,
                on_conflict='market_id_1,market_id_2'
            ).execute()
            
            if response.data:
                return MarketRelation(**response.data[0])
            raise Exception("Failed to create relation")
            
        except Exception as e:
            logger.error(f"Error creating relation: {e}")
            raise
    
    async def create_relations_batch(
        self,
        relations: List[MarketRelationCreate]
    ) -> dict:
        """
        Create multiple relations in batch.
        
        Args:
            relations: List of relations to create
            
        Returns:
            Dictionary with success/failure counts
        """
        created = 0
        failed = 0
        
        for relation in relations:
            try:
                await self.create_relation(
                    relation.market_id_1,
                    relation.market_id_2,
                    relation.similarity,
                    relation.correlation or 0.0,
                    relation.pressure or 0.0
                )
                created += 1
            except Exception as e:
                failed += 1
                logger.error(f"Failed to create relation: {e}")
        
        return {
            "created": created,
            "failed": failed,
            "total": len(relations)
        }
    
    async def delete_relation(
        self,
        market_id_1: int,
        market_id_2: int
    ) -> bool:
        """Delete a relation between two markets."""
        try:
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            response = self.db.client.table('market_relations')\
                .delete()\
                .eq('market_id_1', min_id)\
                .eq('market_id_2', max_id)\
                .execute()
            
            return len(response.data) > 0
            
        except Exception as e:
            logger.error(f"Error deleting relation: {e}")
            raise
    
    async def delete_all_relations_for_market(
        self,
        market_id: int
    ) -> int:
        """Delete all relations involving a specific market."""
        try:
            response = self.db.client.table('market_relations')\
                .delete()\
                .or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")\
                .execute()
            
            return len(response.data)
            
        except Exception as e:
            logger.error(f"Error deleting relations for market: {e}")
            raise
    
    async def count_relations(
        self,
        market_id: Optional[int] = None
    ) -> int:
        """Count total relations, optionally for a specific market."""
        try:
            query = self.db.client.table('market_relations').select('id', count='exact')
            
            if market_id is not None:
                query = query.or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")
            
            response = query.execute()
            return response.count if response.count is not None else 0
            
        except Exception as e:
            logger.error(f"Error counting relations: {e}")
            raise
    
    # ==================== CALCULATION METHODS ====================
    
    def calculate_correlation(self, market1: Market, market2: Market) -> float:
        """
        Calculate correlation between two markets based on outcome prices.
        
        If both markets have outcome prices, calculate correlation.
        Otherwise, return 0.0 (no price data available).
        
        Args:
            market1: First market
            market2: Second market
            
        Returns:
            Correlation score (0.0-1.0)
        """
        return 1.0 # TODO: Implement correlation calculation
    
    def calculate_pressure(
        self,
        similarity: float,
        correlation: float,
        market1: Market,
        market2: Market
    ) -> float:
        """
        Calculate pressure score based on similarity, correlation, and volatility difference.
        
        Pressure represents the "strength" or "intensity" of the relationship.
        Higher volatility difference = higher pressure (more dynamic relationship).
        
        Args:
            similarity: Similarity score (0.0-1.0)
            correlation: Correlation score (0.0-1.0)
            market1: First market object
            market2: Second market object
            
        Returns:
            Pressure score (0.0-1.0)
        """
        volatility1 = self._calculate_volatility_from_price_changes(market1)
        volatility2 = self._calculate_volatility_from_price_changes(market2)
        
        volatility_diff = abs(volatility1 - volatility2)
        
        if volatility_diff <= 0:
            pressure_factor = 0.1
        else:
            pressure_factor = min(1.0, np.sqrt(volatility_diff))
            pressure_factor = max(0.1, pressure_factor)
        
        pressure = similarity * correlation * pressure_factor
        
        return max(0.0, min(1.0, pressure))
    
    def _calculate_volatility_from_price_changes(self, market: Market) -> float:
        """
        Calculate volatility from price change data.
        
        Uses the average of absolute price changes as a measure of volatility.
        
        Args:
            market: Market object with price change data
            
        Returns:
            Volatility score (0.0+, typically 0-1 range)
        """
        price_changes = []
        
        # Collect all available price changes (as absolute values)
        if market.one_day_price_change is not None:
            price_changes.append(abs(market.one_day_price_change))
        
        if market.one_week_price_change is not None:
            price_changes.append(abs(market.one_week_price_change))
        
        if market.one_month_price_change is not None:
            price_changes.append(abs(market.one_month_price_change))
        
        # If no price change data, return 0
        if not price_changes:
            return 0.0
        
        # Return average of absolute price changes as volatility
        return sum(price_changes) / len(price_changes)
    
    # ==================== RELATION DISCOVERY METHODS ====================
    
    async def find_similar_markets_for_relation(
        self,
        market_id: int,
        similarity_threshold: float,
        limit: int = 100
    ) -> List[Tuple[int, float]]:
        """
        Find similar markets using vector embeddings.
        
        Args:
            market_id: Source market ID
            similarity_threshold: Minimum similarity threshold
            limit: Maximum number of results
            
        Returns:
            List of (market_id, similarity) tuples
        """
        try:
            results = await self.vector_service.find_similar_to_market(market_id, limit=limit)
            
            # Filter by threshold
            filtered = [(mid, sim) for mid, sim in results if sim >= similarity_threshold]
            return filtered
            
        except Exception as e:
            logger.error(f"Error finding similar markets for {market_id}: {e}")
            return []
    
    async def create_relations_for_market(
        self,
        market_id: int,
        similarity_threshold: float,
        correlation_threshold: float,
        limit: int = 100
    ) -> Tuple[int, int]:
        """
        Create relations for a single market based on similarity and correlation thresholds.
        
        Args:
            market_id: Market ID to create relations for
            similarity_threshold: Minimum similarity threshold (0.0-1.0)
            correlation_threshold: Minimum correlation threshold (0.0-1.0)
            limit: Maximum number of similar markets to consider
            
        Returns:
            Tuple of (created_count, skipped_count)
        """
        created = 0
        skipped = 0
        
        try:
            # Get the market
            market = await self.db.get_market_by_id(market_id)
            if not market:
                logger.warning(f"Market {market_id} not found")
                return (0, 0)
            
            # Find similar markets
            similar_markets = await self.find_similar_markets_for_relation(
                market_id,
                similarity_threshold,
                limit=limit
            )
            
            if not similar_markets:
                return (0, 0)
            
            # Check existing relations to avoid duplicates
            existing_relations = await self.get_related_markets(market_id, limit=1000, min_similarity=0.0)
            existing_market_ids = {mid for mid, _, _, _ in existing_relations}
            
            # Process each similar market
            relations_to_create = []
            
            for similar_market_id, similarity in similar_markets:
                # Skip if relation already exists
                if similar_market_id in existing_market_ids:
                    skipped += 1
                    continue
                
                # Skip self
                if similar_market_id == market_id:
                    continue
                
                # Get the similar market
                similar_market = await self.db.get_market_by_id(similar_market_id)
                if not similar_market:
                    continue
                
                # Calculate correlation
                correlation = self.calculate_correlation(market, similar_market)
                
                # Skip if correlation is below threshold
                if correlation < correlation_threshold:
                    skipped += 1
                    continue
                
                # Calculate pressure
                pressure = self.calculate_pressure(
                    similarity=similarity,
                    correlation=correlation,
                    market1=market,
                    market2=similar_market
                )
                
                # Create relation
                relations_to_create.append(
                    MarketRelationCreate(
                        market_id_1=market_id,
                        market_id_2=similar_market_id,
                        similarity=similarity,
                        correlation=correlation,
                        pressure=pressure
                    )
                )
            
            # Batch create relations
            if relations_to_create:
                result = await self.create_relations_batch(relations_to_create)
                created = result.get('created', 0)
                skipped += result.get('failed', 0)
            
            return (created, skipped)
            
        except Exception as e:
            logger.error(f"Error creating relations for market {market_id}: {e}")
            return (0, 0)
    
    async def estimate_relations_count(
        self,
        market_ids: List[int],
        similarity_threshold: float,
        correlation_threshold: float,
        sample_size: Optional[int] = None
    ) -> dict:
        """
        Estimate how many relations would be created for given markets.
        Uses sampling if sample_size is provided for faster estimation.
        
        Args:
            market_ids: List of market IDs to estimate for
            similarity_threshold: Minimum similarity threshold
            correlation_threshold: Minimum correlation threshold
            sample_size: If provided, only sample this many markets for estimation
            
        Returns:
            Dictionary with estimation results:
            - estimated_total: Estimated total relations
            - sampled_markets: Number of markets sampled
            - relations_per_market: Average relations per market
            - sample_relations: Actual relations found in sample
        """
        try:
            # If sample_size is provided, use sampling
            markets_to_sample = market_ids
            if sample_size and sample_size < len(market_ids):
                import random
                markets_to_sample = random.sample(market_ids, min(sample_size, len(market_ids)))
            
            total_relations = 0
            markets_processed = 0
            
            for market_id in markets_to_sample:
                try:
                    # Get the market
                    market = await self.db.get_market_by_id(market_id)
                    if not market:
                        continue
                    
                    # Find similar markets
                    similar_markets = await self.find_similar_markets_for_relation(
                        market_id,
                        similarity_threshold,
                        limit=100
                    )
                    
                    if not similar_markets:
                        continue
                    
                    # Check existing relations to avoid duplicates
                    existing_relations = await self.get_related_markets(market_id, limit=1000, min_similarity=0.0)
                    existing_market_ids = {mid for mid, _, _, _ in existing_relations}
                    
                    # Count potential relations
                    potential_relations = 0
                    
                    for similar_market_id, similarity in similar_markets:
                        # Skip if relation already exists
                        if similar_market_id in existing_market_ids:
                            continue
                        
                        # Skip self
                        if similar_market_id == market_id:
                            continue
                        
                        # Get the similar market
                        similar_market = await self.db.get_market_by_id(similar_market_id)
                        if not similar_market:
                            continue
                        
                        # Calculate correlation
                        correlation = self.calculate_correlation(market, similar_market)
                        
                        # Count if correlation meets threshold
                        if correlation >= correlation_threshold:
                            potential_relations += 1
                    
                    total_relations += potential_relations
                    markets_processed += 1
                    
                except Exception as e:
                    logger.debug(f"Error estimating for market {market_id}: {e}")
                    continue
            
            # Calculate average and extrapolate
            if markets_processed > 0:
                avg_relations_per_market = total_relations / markets_processed
                estimated_total = int(avg_relations_per_market * len(market_ids))
            else:
                avg_relations_per_market = 0.0
                estimated_total = 0
            
            return {
                "estimated_total": estimated_total,
                "sampled_markets": markets_processed,
                "total_markets": len(market_ids),
                "relations_per_market": round(avg_relations_per_market, 2),
                "sample_relations": total_relations,
                "is_sampled": sample_size is not None and sample_size < len(market_ids)
            }
            
        except Exception as e:
            logger.error(f"Error estimating relations count: {e}")
            return {
                "estimated_total": 0,
                "sampled_markets": 0,
                "total_markets": len(market_ids),
                "relations_per_market": 0.0,
                "sample_relations": 0,
                "is_sampled": False,
                "error": str(e)
            }


    async def get_graph_data(
        self,
        limit: int = 100,
        min_similarity: float = 0.7,
        is_active: Optional[bool] = True
    ) -> dict:
        """
        Get market graph data for visualization (nodes + connections).
        
        Args:
            limit: Maximum number of markets to include
            min_similarity: Minimum similarity for connections
            is_active: Filter by active status
            
        Returns:
            Dictionary with 'markets' and 'relations' lists
        """
        try:
            # Step 1: Get markets
            markets = await self.db.get_markets(
                limit=limit,
                is_active=is_active,
                order_by='volume',
                ascending=False
            )
            
            if not markets:
                return {'markets': [], 'relations': []}
            
            # Get all market IDs
            market_ids = [m.id for m in markets]
            
            # Step 2: Get all relations involving these markets (efficiently)
            # Query relations where any of these IDs are involved
            query1 = self.db.client.table('market_relations').select('*').in_('market_id_1', market_ids)
            query2 = self.db.client.table('market_relations').select('*').in_('market_id_2', market_ids)
            
            # Apply similarity filter
            if min_similarity is not None:
                query1 = query1.gte('similarity', min_similarity)
                query2 = query2.gte('similarity', min_similarity)
            
            # Execute queries
            response1 = query1.execute()
            response2 = query2.execute()
            
            # Combine and deduplicate relations
            seen_ids = set()
            relations = []
            market_id_set = set(market_ids)
            
            for relation_data in response1.data + response2.data:
                relation_id = relation_data['id']
                # Only include if both markets are in our set (for visualization)
                if (relation_data['market_id_1'] in market_id_set and 
                    relation_data['market_id_2'] in market_id_set and 
                    relation_id not in seen_ids):
                    seen_ids.add(relation_id)
                    relations.append(MarketRelation(**relation_data))
            
            return {
                'markets': markets,
                'relations': relations
            }
            
        except Exception as e:
            logger.error(f"Error getting graph data: {e}")
            raise
    
    async def get_relations_by_polymarket_ids(
        self,
        polymarket_ids: List[str],
        min_similarity: Optional[float] = None
    ) -> Tuple[List[MarketRelation], List[str], int]:
        """
        Efficiently retrieve all market relations where any of the given polymarket IDs are involved.
        
        Args:
            polymarket_ids: List of polymarket IDs to find relations for
            min_similarity: Optional minimum similarity threshold
            
        Returns:
            Tuple of (relations, markets_not_found, markets_found_count)
        """
        try:
            # Step 1: Batch convert polymarket_ids to database IDs
            response = self.db.client.table('markets').select('id, polymarket_id').in_(
                'polymarket_id', polymarket_ids
            ).execute()
            
            if not response.data:
                return ([], polymarket_ids, 0)
            
            # Create mappings
            market_ids = [row['id'] for row in response.data]
            found_polymarket_ids = {row['polymarket_id'] for row in response.data}
            markets_not_found = [pm_id for pm_id in polymarket_ids if pm_id not in found_polymarket_ids]
            
            # Step 2: Query market_relations where any of these IDs are involved
            # Use two separate queries with IN clauses for better performance
            query1 = self.db.client.table('market_relations').select('*').in_('market_id_1', market_ids)
            query2 = self.db.client.table('market_relations').select('*').in_('market_id_2', market_ids)
            
            # Apply similarity filter if provided
            if min_similarity is not None:
                query1 = query1.gte('similarity', min_similarity)
                query2 = query2.gte('similarity', min_similarity)
            
            # Execute both queries
            response1 = query1.execute()
            response2 = query2.execute()
            
            # Combine results and remove duplicates (a relation might appear in both)
            seen_ids = set()
            relations = []
            
            for relation_data in response1.data + response2.data:
                relation_id = relation_data['id']
                if relation_id not in seen_ids:
                    seen_ids.add(relation_id)
                    relations.append(MarketRelation(**relation_data))
            
            # Sort by similarity descending
            relations.sort(key=lambda r: r.similarity, reverse=True)
            
            return (relations, markets_not_found, len(market_ids))
            
        except Exception as e:
            logger.error(f"Error retrieving batch relations: {e}")
            raise


_relation_service: Optional[RelationService] = None


def get_relation_service() -> RelationService:
    """Get or create the relation service singleton."""
    global _relation_service
    if _relation_service is None:
        _relation_service = RelationService()
    return _relation_service

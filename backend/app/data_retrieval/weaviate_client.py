import weaviate
from weaviate.classes.init import Auth
import logging
from typing import List, Dict, Any
import time

logger = logging.getLogger(__name__)

class WeaviateClient:
    def __init__(self, url: str, api_key: str):
        """Initialize connection to Weaviate database."""
        logger.info("=" * 80)
        logger.info("Initializing Weaviate client (v4)")
        logger.info(f"URL: {url}")
        logger.info(f"API Key: {'*' * 10}{api_key[-4:] if api_key and len(api_key) > 4 else 'None'}")
        
        try:
            # Weaviate v4 API syntax
            self.client = weaviate.connect_to_weaviate_cloud(
                cluster_url=url,
                auth_credentials=Auth.api_key(api_key)
            )
            
            # Test connection
            logger.info("Testing Weaviate connection...")
            is_ready = self.client.is_ready()
            
            if is_ready:
                logger.info("✓ Successfully connected to Weaviate")
                logger.info("✓ Weaviate cluster is ready")
            else:
                logger.warning("⚠ Connected to Weaviate but cluster is not ready")
                
        except Exception as e:
            logger.error("✗ Failed to connect to Weaviate")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {e}", exc_info=True)
            raise
        
        logger.info("=" * 80)

    def create_market_schema(self):
        """Create or verify the Market schema in Weaviate."""
        from weaviate.classes.config import Property, DataType
        
        logger.info("-" * 80)
        logger.info("Setting up Market schema in Weaviate")
        
        try:
            # Check if collection already exists
            if self.client.collections.exists("Market"):
                logger.info("✓ Market collection already exists")
                collection = self.client.collections.get("Market")
                logger.info(f"  Collection retrieved successfully")
            else:
                logger.info("Creating new Market collection...")
                self.client.collections.create(
                    name="Market",
                    description="A prediction market from Polymarket",
                    properties=[
                        Property(name="question", data_type=DataType.TEXT),
                        Property(name="description", data_type=DataType.TEXT),
                        Property(name="outcomes", data_type=DataType.TEXT_ARRAY),
                        Property(name="outcome_prices", data_type=DataType.TEXT_ARRAY),
                        Property(name="end_date", data_type=DataType.DATE),
                        Property(name="volume", data_type=DataType.NUMBER),
                        Property(name="is_active", data_type=DataType.BOOL),
                    ]
                )
                logger.info("✓ Market collection created successfully")
                
        except Exception as e:
            logger.warning(f"Schema creation issue: {type(e).__name__}: {e}")
            logger.info("Continuing (collection may already exist)...")
        
        logger.info("-" * 80)

    def import_markets(self, markets: List[Dict[str, Any]]):
        """Batch import markets into Weaviate."""
        logger.info("=" * 80)
        logger.info(f"Starting batch import of {len(markets)} markets to Weaviate")
        logger.info("=" * 80)
        
        if not markets:
            logger.warning("No markets to import!")
            return
        
        start_time = time.time()
        successful = 0
        failed = 0
        
        try:
            logger.info("Getting Market collection...")
            market_collection = self.client.collections.get("Market")
            
            logger.info("Starting batch insert...")
            with market_collection.batch.dynamic() as batch:
                for i, market in enumerate(markets, 1):
                    try:
                        batch.add_object(properties=market)
                        successful += 1
                        
                        if i % 100 == 0:
                            logger.info(f"Progress: {i}/{len(markets)} markets processed ({(i/len(markets)*100):.1f}%)")
                            
                    except Exception as e:
                        failed += 1
                        logger.error(f"Failed to add market {i}: {e}")
                        logger.debug(f"Market data: {market.get('question', 'N/A')[:50]}")
            
            elapsed = time.time() - start_time
            
            logger.info("=" * 80)
            logger.info("Batch import complete")
            logger.info(f"✓ Successfully imported: {successful} markets")
            if failed > 0:
                logger.warning(f"✗ Failed to import: {failed} markets")
            logger.info(f"⏱ Time elapsed: {elapsed:.2f} seconds")
            if elapsed > 0:
                logger.info(f"⚡ Average speed: {successful/elapsed:.1f} markets/second")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("✗ Batch import failed")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {e}", exc_info=True)
            logger.error(f"Successfully imported before error: {successful}")
            logger.error("=" * 80)
            raise


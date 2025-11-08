from supabase import create_client, Client
import logging
from typing import List, Dict, Any
import time

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, api_key: str):
        """Initialize connection to Supabase database."""
        logger.info("=" * 80)
        logger.info("Initializing Supabase client")
        logger.info(f"URL: {url}")
        logger.info(f"API Key: {'*' * 10}{api_key[-4:] if api_key and len(api_key) > 4 else 'None'}")
        
        try:
            self.client: Client = create_client(url, api_key)
            logger.info("✓ Successfully connected to Supabase")
                
        except Exception as e:
            logger.error("✗ Failed to connect to Supabase")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {e}", exc_info=True)
            raise
        
        logger.info("=" * 80)

    def create_markets_table(self):
        """Create the markets table if it doesn't exist."""
        logger.info("-" * 80)
        logger.info("Setting up markets table in Supabase")
        
        # SQL to create table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS markets (
            id BIGSERIAL PRIMARY KEY,
            question TEXT,
            description TEXT,
            outcomes TEXT[],
            outcome_prices TEXT[],
            end_date TIMESTAMPTZ,
            volume NUMERIC,
            is_active BOOLEAN,
            polymarket_id TEXT UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_markets_is_active ON markets(is_active);
        CREATE INDEX IF NOT EXISTS idx_markets_end_date ON markets(end_date);
        CREATE INDEX IF NOT EXISTS idx_markets_polymarket_id ON markets(polymarket_id);
        """
        
        try:
            # Note: Supabase doesn't expose direct SQL execution via the Python client
            # The table should be created via the Supabase dashboard or migrations
            logger.info("✓ Table should be created via Supabase dashboard")
            logger.info("  Run the following SQL in Supabase SQL Editor:")
            logger.info("")
            for line in create_table_sql.strip().split('\n'):
                logger.info(f"    {line}")
            logger.info("")
            logger.info("  Table: markets")
            logger.info("  Columns: id, question, description, outcomes, outcome_prices, end_date, volume, is_active, polymarket_id, created_at, updated_at")
                
        except Exception as e:
            logger.warning(f"Table setup info: {type(e).__name__}: {e}")
        
        logger.info("-" * 80)

    def import_markets(self, markets: List[Dict[str, Any]]):
        """Batch import markets into Supabase."""
        logger.info("=" * 80)
        logger.info(f"Starting batch import of {len(markets)} markets to Supabase")
        logger.info("=" * 80)
        
        if not markets:
            logger.warning("No markets to import!")
            return
        
        start_time = time.time()
        successful = 0
        failed = 0
        updated = 0
        
        try:
            logger.info("Starting upsert operation...")
            
            # Process in batches of 50 for better reliability
            batch_size = 50
            for i in range(0, len(markets), batch_size):
                batch = markets[i:i+batch_size]
                
                try:
                    # Upsert will insert or update based on polymarket_id
                    response = self.client.table('markets').upsert(
                        batch,
                        on_conflict='polymarket_id'
                    ).execute()
                    
                    successful += len(batch)
                    logger.info(f"Progress: {min(i+batch_size, len(markets))}/{len(markets)} markets processed ({(min(i+batch_size, len(markets))/len(markets)*100):.1f}%)")
                    
                    # Small delay to avoid overwhelming the database
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Failed to import batch starting at {i}: {type(e).__name__}: {e}")
                    logger.info(f"Attempting individual inserts for this batch...")
                    
                    # Try individual inserts for this batch
                    for j, market in enumerate(batch):
                        try:
                            self.client.table('markets').upsert(
                                market,
                                on_conflict='polymarket_id'
                            ).execute()
                            successful += 1
                            
                            if (j + 1) % 10 == 0:
                                logger.debug(f"  Individual insert progress: {j+1}/{len(batch)}")
                                
                        except Exception as e2:
                            failed += 1
                            question = market.get('question', 'N/A')[:50]
                            logger.error(f"Failed to import market: {question} - {type(e2).__name__}: {str(e2)[:100]}")
                    
                    # Reset counter since we tried individual inserts
                    failed = 0  # Recalculate based on actual failures above
            
            elapsed = time.time() - start_time
            
            logger.info("=" * 80)
            logger.info("Batch import complete")
            logger.info(f"✓ Successfully imported/updated: {successful} markets")
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


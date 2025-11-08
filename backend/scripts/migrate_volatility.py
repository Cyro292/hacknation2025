"""
Migration script to calculate volatility for existing markets.
Respects Polymarket rate limits and stores in separate volatility table.
"""
import asyncio
import logging
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from supabase import create_client
from app.data_retrieval.polymarket_api_enhanced import PolymarketVolatilityCalculator
from dotenv import load_dotenv
import json

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def migrate_volatility():
    """
    Migrate volatility scores for all markets.
    Only calculates if not already in market_volatility table.
    """
    # Initialize clients
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_API_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("Missing SUPABASE_URL or SUPABASE_API_KEY")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    calculator = PolymarketVolatilityCalculator()
    
    try:
        logger.info("=" * 80)
        logger.info("STARTING VOLATILITY MIGRATION")
        logger.info("=" * 80)
        
        # Get all active markets with price change data
        logger.info("Fetching markets from database...")
        response = supabase.table('markets').select('*').eq('is_active', True).execute()
        
        markets_to_process = response.data
        logger.info(f"Found {len(markets_to_process)} active markets to process")
        
        if not markets_to_process:
            logger.info("⚠️  No active markets found!")
        
        # Process markets
        successful = 0
        failed = 0
        price_24h_count = 0
        price_7d_count = 0
        price_30d_count = 0
        proxy_count = 0
        
        logger.info("\n" + "=" * 80)
        logger.info(f"PROCESSING {len(markets_to_process)} MARKETS")
        logger.info("Rate limit: 95 requests per 10 seconds")
        logger.info(f"Estimated time: {len(markets_to_process) * 0.105:.1f} seconds ({len(markets_to_process) * 0.105 / 60:.1f} minutes)")
        logger.info("=" * 80 + "\n")
        
        batch_size = 50
        for batch_idx in range(0, len(markets_to_process), batch_size):
            batch = markets_to_process[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            total_batches = (len(markets_to_process) + batch_size - 1) // batch_size
            
            logger.info(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} markets)...")
            
            for i, market in enumerate(batch):
                try:
                    market_id = market['id']
                    polymarket_id = market['polymarket_id']
                    
                    # Try to use real price change data first (24h, 7d, or 30d)
                    real_volatility, method, metadata = calculator.calculate_volatility_from_price_changes(market)
                    
                    # Track which data source was used
                    if real_volatility is not None:
                        if method == "price_change_24h":
                            price_24h_count += 1
                        elif method == "price_change_7d_scaled":
                            price_7d_count += 1
                        elif method == "price_change_30d_scaled":
                            price_30d_count += 1
                    
                    # Also calculate proxy for markets without real data
                    proxy_volatility = None
                    if real_volatility is None:
                        proxy_volatility, proxy_method, proxy_metadata = calculator.calculate_proxy_volatility(market)
                        proxy_count += 1
                    
                    # Insert into volatility table (store both real and proxy in separate columns)
                    insert_data = {
                        'market_id': market_id,
                        'polymarket_id': polymarket_id,
                        'real_volatility_24h': real_volatility,
                        'proxy_volatility_24h': proxy_volatility,
                        'calculation_method': method if real_volatility else proxy_method,
                        'data_points': metadata.get('data_points', 0) if real_volatility else proxy_metadata.get('data_points', 0),
                        'price_range_24h': json.dumps(metadata.get('price_range', {}) if real_volatility else proxy_metadata.get('price_range', {})),
                        'calculated_at': datetime.now().isoformat()
                    }
                    
                    supabase.table('market_volatility').upsert(
                        insert_data,
                        on_conflict='market_id'
                    ).execute()
                    
                    successful += 1
                    
                    if (i + 1) % 10 == 0:
                        progress = (batch_idx + i + 1) / len(markets_to_process) * 100
                        logger.info(
                            f"  Progress: {batch_idx + i + 1}/{len(markets_to_process)} "
                            f"({progress:.1f}%) - "
                            f"✓ {successful} successful, ✗ {failed} failed"
                        )
                    
                except Exception as e:
                    failed += 1
                    logger.error(f"  ✗ Error processing market {market.get('polymarket_id')}: {e}")
            
            logger.info(f"  ✓ Batch {batch_num} complete")
        
        logger.info("\n" + "=" * 80)
        logger.info("MIGRATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"✓ Successfully calculated: {successful}")
        logger.info(f"  Real volatility from price data:")
        logger.info(f"    ├─ 24h price change (best): {price_24h_count}")
        logger.info(f"    ├─ 7d price change (scaled): {price_7d_count}")
        logger.info(f"    └─ 30d price change (scaled): {price_30d_count}")
        logger.info(f"  Proxy volatility (fallback): {proxy_count}")
        logger.info(f"✗ Failed: {failed}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
    
    finally:
        await calculator.close()


if __name__ == "__main__":
    asyncio.run(migrate_volatility())


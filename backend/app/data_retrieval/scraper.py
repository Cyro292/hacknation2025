import logging
from .polymarket_api import PolymarketAPI
from .supabase_client import SupabaseClient
from .scrape_tracker import ScrapeTracker
from .polymarket_api_enhanced import PolymarketVolatilityCalculator
from ..schemas.market_schema import MarketCreate
from datetime import datetime
import time
import json
import asyncio

logger = logging.getLogger(__name__)

def scrape_and_store_markets(supabase_url: str, supabase_api_key: str):
    """
    Scrapes active markets from Polymarket and stores them in Supabase.
    Includes deduplication and distributed scrape tracking to prevent duplicate runs.
    """
    logger.info("\n")
    logger.info("🚀" * 40)
    logger.info(f"STARTING DATA SCRAPING CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🚀" * 40)
    
    start_time = time.time()
    tracker = None
    markets_fetched = 0
    markets_added = 0
    markets_failed = 0
    
    try:
        # Initialize clients
        logger.info("\n📡 Step 1/7: Initializing API clients...")
        polymarket_api = PolymarketAPI()
        supabase = SupabaseClient(supabase_url, supabase_api_key)
        
        # Initialize scrape tracker
        tracker = ScrapeTracker(supabase.client)
        
        # Clean up any stale scrapes
        tracker.cleanup_stale_scrapes()
        
        # Check if we should run the scrape
        logger.info("\n🔍 Step 2/7: Checking scrape eligibility...")
        should_run, reason = tracker.should_run_scrape(min_interval_minutes=55)
        
        if not should_run:
            logger.warning(f"⏸️  SCRAPE SKIPPED: {reason}")
            logger.info("=" * 80)
            return
        
        logger.info(f"✓ Scrape approved: {reason}")
        
        # Start tracking this scrape
        scrape_id = tracker.start_scrape()
        if not scrape_id:
            logger.warning("⚠️  Could not start scrape tracking, continuing anyway...")
        
        # Ensure the table exists
        logger.info("\n🗄️  Step 3/7: Setting up database tables...")
        supabase.create_markets_table()

        # Scrape active markets
        logger.info("\n📥 Step 4/7: Fetching markets from Polymarket API...")
        active_markets = polymarket_api.get_active_markets()
        
        if not active_markets:
            logger.warning("⚠️  No active markets found!")
            logger.warning("This might indicate an API issue or all markets are closed.")
            return

        markets_fetched = len(active_markets)
        
        # Prepare data for Supabase
        logger.info("\n🔄 Step 5/7: Preparing and importing data to Supabase...")
        logger.info(f"Processing {markets_fetched} markets...")
        
        markets_to_import = []
        skipped = 0
        
        for i, market in enumerate(active_markets, 1):
            try:
                # Generate a unique ID from Polymarket's data
                polymarket_id = market.get("id") or market.get("condition_id") or f"market_{i}"
                
                # Get raw data from API
                outcomes = market.get("outcomes", [])
                outcome_prices = market.get("outcomePrices", [])
                
                # Ensure arrays are proper lists, not strings
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except:
                        outcomes = [outcomes]
                
                if isinstance(outcome_prices, str):
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except:
                        outcome_prices = [outcome_prices]
                
                # Skip inactive markets
                if not market.get("active", True):
                    skipped += 1
                    continue
                
                # Create market data using Pydantic schema for validation
                try:
                    market_schema = MarketCreate(
                        polymarket_id=str(polymarket_id),
                        question=market.get("question") or "",
                        description=market.get("description"),
                        outcomes=outcomes if isinstance(outcomes, list) else [],
                        outcome_prices=[str(p) for p in outcome_prices] if isinstance(outcome_prices, list) else [],
                        end_date=market.get("endDate"),
                        volume=float(market.get("volume", 0)) if market.get("volume") else 0.0,
                        is_active=market.get("active", True),
                        slug=market.get("slug"),
                        one_day_price_change=market.get("oneDayPriceChange"),
                        one_week_price_change=market.get("oneWeekPriceChange"),
                        one_month_price_change=market.get("oneMonthPriceChange"),
                    )
                    
                    # Convert validated schema to dict for database insertion
                    # mode='json' ensures datetime objects are serialized to ISO format strings
                    market_data = market_schema.model_dump(mode='json')
                    
                except Exception as validation_error:
                    logger.warning(f"Skipping market {i}: Validation failed - {validation_error}")
                    skipped += 1
                    continue
                
                # Validate that essential fields are present
                if not market_data["question"]:
                    logger.warning(f"Skipping market {i}: Missing question")
                    skipped += 1
                    continue
                    
                markets_to_import.append(market_data)
                
                if i % 500 == 0:
                    logger.info(f"Processed {i}/{len(active_markets)} markets...")
                    
            except Exception as e:
                skipped += 1
                logger.error(f"Error processing market {i}: {e}")
                logger.debug(f"Market data: {market}")

        logger.info(f"\n✅ Prepared {len(markets_to_import)} markets for import")
        if skipped > 0:
            logger.warning(f"⚠️  Skipped {skipped} markets due to errors or missing data")

        # Import data into Supabase
        if markets_to_import:
            supabase.import_markets(markets_to_import)
            markets_added = len(markets_to_import)
            markets_failed = skipped
            
            # Calculate volatility for new/updated markets
            logger.info("\n📊 Step 6/7: Calculating volatility scores...")
            try:
                async def calculate_volatility_async():
                    calculator = PolymarketVolatilityCalculator()
                    try:
                        # Get market IDs that were just imported
                        polymarket_ids = [m['polymarket_id'] for m in markets_to_import]
                        
                        # Check which already have volatility
                        from supabase import create_client
                        supabase_client = create_client(supabase_url, supabase_api_key)
                        
                        existing_response = supabase_client.table('market_volatility').select('polymarket_id').in_('polymarket_id', polymarket_ids).execute()
                        existing_polymarket_ids = {row['polymarket_id'] for row in existing_response.data}
                        
                        # Filter to only calculate for new markets
                        markets_needing_volatility = [
                            m for m in markets_to_import 
                            if m['polymarket_id'] not in existing_polymarket_ids
                        ]
                        
                        if not markets_needing_volatility:
                            logger.info("  All markets already have volatility scores")
                            return 0, 0
                        
                        logger.info(f"  Calculating volatility for {len(markets_needing_volatility)} new markets...")
                        logger.info(f"  Using price change data from Gamma API (no rate limits!)")
                        
                        vol_success = 0
                        price_history_count = 0
                        proxy_count = 0
                        
                        for i, market in enumerate(markets_needing_volatility):
                            try:
                                polymarket_id = market['polymarket_id']
                                
                                # Try to use real price change data first (BEST method!)
                                volatility, method, metadata = calculator.calculate_volatility_from_price_changes(market)
                                
                                if volatility is None:
                                    # Fallback to proxy if no price change data
                                    volatility, method, metadata = calculator.calculate_proxy_volatility(market)
                                    proxy_count += 1
                                else:
                                    price_history_count += 1
                                
                                # Get market DB ID
                                market_response = supabase_client.table('markets').select('id').eq('polymarket_id', polymarket_id).execute()
                                if not market_response.data:
                                    continue
                                
                                market_id = market_response.data[0]['id']
                                
                                # Insert volatility
                                insert_data = {
                                    'market_id': market_id,
                                    'polymarket_id': polymarket_id,
                                    'volatility_24h': volatility,
                                    'calculation_method': method,
                                    'data_points': metadata.get('data_points', 0),
                                    'price_range_24h': json.dumps(metadata.get('price_range', {})),
                                    'calculated_at': datetime.now().isoformat()
                                }
                                
                                supabase_client.table('market_volatility').upsert(
                                    insert_data,
                                    on_conflict='market_id'
                                ).execute()
                                
                                vol_success += 1
                                
                                if (i + 1) % 50 == 0:
                                    logger.info(f"    Progress: {i+1}/{len(markets_needing_volatility)} ({(i+1)/len(markets_needing_volatility)*100:.1f}%)")
                                
                            except Exception as e:
                                logger.debug(f"    Error calculating volatility for {market.get('polymarket_id')}: {e}")
                        
                        return vol_success, price_history_count
                        
                    finally:
                        await calculator.close()
                
                        # Run async function
                        vol_success, price_history_count = asyncio.run(calculate_volatility_async())
                        logger.info(f"✅ Calculated volatility for {vol_success} markets ({price_history_count} from real price changes)")
                
            except Exception as e:
                logger.warning(f"⚠️  Volatility calculation failed (non-critical): {e}")
            
            # Create embeddings for new markets
            logger.info("\n🧠 Step 7/7: Creating embeddings for markets...")
            try:
                from ..services.vector_service import get_vector_service
                from ..services.database_service import get_database_service
                
                # Use async wrapper for embedding creation
                import asyncio
                
                async def create_embeddings_async():
                    vs = get_vector_service()
                    db = get_database_service()
                    
                    # Get all markets
                    markets = await db.get_markets(limit=10000)
                    
                    # Find markets that need embeddings (BATCH CHECK!)
                    all_embeddings = await db.get_all_embeddings(limit=100000)
                    existing_ids = {emb.market_id for emb in all_embeddings}
                    needs_embedding = [m.id for m in markets if m.id not in existing_ids]
                    
                    if not needs_embedding:
                        logger.info("  All markets already have embeddings")
                        return 0
                    
                    logger.info(f"  Creating embeddings for {len(needs_embedding)} markets in batches...")
                    
                    # Batch create embeddings (much faster!)
                    result = await vs.batch_create_embeddings(needs_embedding, batch_size=100)
                    
                    return result['created']
                
                # Run async function
                embeddings_created = asyncio.run(create_embeddings_async())
                logger.info(f"✅ Created {embeddings_created} new embeddings")
                
            except Exception as e:
                logger.warning(f"⚠️  Embedding creation failed (non-critical): {e}")
        else:
            logger.warning("⚠️  No valid markets to import after processing!")

        elapsed = time.time() - start_time
        
        # Mark scrape as completed
        if tracker:
            tracker.complete_scrape(
                markets_fetched=markets_fetched,
                markets_added=markets_added,
                markets_updated=markets_added,  # Upsert means this could be adds or updates
                markets_failed=markets_failed,
                duration_seconds=elapsed
            )
        
        logger.info("\n")
        logger.info("✅" * 40)
        logger.info(f"SCRAPING CYCLE COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
        logger.info("✅" * 40)
        logger.info("\n")

    except Exception as e:
        elapsed = time.time() - start_time
        
        # Mark scrape as failed
        if tracker:
            tracker.fail_scrape(
                error_message=str(e),
                duration_seconds=elapsed
            )
        
        logger.error("\n")
        logger.error("❌" * 40)
        logger.error(f"SCRAPING CYCLE FAILED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {e}", exc_info=True)
        logger.error(f"Time before failure: {elapsed:.2f} seconds")
        logger.error("❌" * 40)
        logger.error("\n")

if __name__ == "__main__":
    # This allows for manual execution of the scraper
    # For automated execution, this will be called by the background task manager
    WEAVIATE_URL = "http://localhost:8080"  # Example URL
    WEAVIATE_API_KEY = "your-weaviate-api-key"
    scrape_and_store_markets(WEAVIATE_URL, WEAVIATE_API_KEY)

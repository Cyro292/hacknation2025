import logging
from .polymarket_api import PolymarketAPI
from .supabase_client import SupabaseClient
from datetime import datetime
import time

logger = logging.getLogger(__name__)

def scrape_and_store_markets(supabase_url: str, supabase_api_key: str):
    """
    Scrapes active markets from Polymarket and stores them in Weaviate.
    """
    logger.info("\n")
    logger.info("🚀" * 40)
    logger.info(f"STARTING DATA SCRAPING CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🚀" * 40)
    
    start_time = time.time()
    
    try:
        # Initialize clients
        logger.info("\n📡 Step 1/4: Initializing API clients...")
        polymarket_api = PolymarketAPI()
        supabase = SupabaseClient(supabase_url, supabase_api_key)

        # Ensure the table exists
        logger.info("\n🗄️  Step 2/4: Setting up database table...")
        supabase.create_markets_table()

        # Scrape active markets
        logger.info("\n📥 Step 3/4: Fetching markets from Polymarket API...")
        active_markets = polymarket_api.get_active_markets()
        
        if not active_markets:
            logger.warning("⚠️  No active markets found!")
            logger.warning("This might indicate an API issue or all markets are closed.")
            return

        # Prepare data for Supabase
        logger.info("\n🔄 Step 4/4: Preparing and importing data to Supabase...")
        logger.info(f"Processing {len(active_markets)} markets...")
        
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
                    import json
                    try:
                        outcomes = json.loads(outcomes)
                    except:
                        outcomes = [outcomes]
                
                if isinstance(outcome_prices, str):
                    import json
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except:
                        outcome_prices = [outcome_prices]
                
                market_data = {
                    "polymarket_id": str(polymarket_id),
                    "question": market.get("question"),
                    "description": market.get("description"),
                    "outcomes": outcomes if isinstance(outcomes, list) else [],
                    "outcome_prices": [str(p) for p in outcome_prices] if isinstance(outcome_prices, list) else [],
                    "end_date": market.get("endDate"),
                    "volume": float(market.get("volume", 0)) if market.get("volume") else 0,
                    "is_active": market.get("active", True),
                }
                
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
        else:
            logger.warning("⚠️  No valid markets to import after processing!")

        elapsed = time.time() - start_time
        logger.info("\n")
        logger.info("✅" * 40)
        logger.info(f"SCRAPING CYCLE COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
        logger.info("✅" * 40)
        logger.info("\n")

    except Exception as e:
        elapsed = time.time() - start_time
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

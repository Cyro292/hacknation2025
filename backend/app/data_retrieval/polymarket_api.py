import httpx
import logging
from typing import List, Dict, Any
import time

logger = logging.getLogger(__name__)

class PolymarketAPI:
    def __init__(self, base_url: str = "https://gamma-api.polymarket.com", rate_limit_delay: float = 0.5):
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay  # Delay between API calls in seconds
        logger.info(f"Initialized PolymarketAPI with base URL: {base_url}")
        logger.info(f"Rate limit delay: {rate_limit_delay}s between requests")

    def get_active_markets(self) -> List[Dict[str, Any]]:
        """
        Fetch all active markets from Polymarket by paginating through events.
        Returns a list of market dictionaries.
        """
        logger.info("=" * 80)
        logger.info("Starting Polymarket data retrieval process")
        logger.info("=" * 80)
        
        markets = []
        offset = 0
        limit = 100
        page = 1
        total_events_processed = 0
        
        while True:
            try:
                params = {
                    "order": "id",
                    "ascending": "false",
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                }
                
                logger.info(f"Fetching page {page} (offset={offset}, limit={limit})...")
                
                # Add rate limiting delay
                if page > 1:
                    time.sleep(self.rate_limit_delay)
                
                response = httpx.get(f"{self.base_url}/events", params=params, timeout=30.0)
                response.raise_for_status()
                
                events = response.json()
                num_events = len(events)
                
                if not events:
                    logger.info(f"No more events found. Pagination complete.")
                    break
                
                logger.info(f"Retrieved {num_events} events on page {page}")
                total_events_processed += num_events
                
                markets_before = len(markets)
                for i, event in enumerate(events):
                    event_markets = event.get("markets", [])
                    markets.extend(event_markets)
                    if event_markets:
                        logger.debug(f"  Event {i+1}/{num_events}: '{event.get('title', 'N/A')}' - {len(event_markets)} markets")
                
                markets_added = len(markets) - markets_before
                logger.info(f"Added {markets_added} markets from page {page} (total so far: {len(markets)})")
                
                offset += limit
                page += 1

            except httpx.TimeoutException as e:
                logger.error(f"Timeout error on page {page}: {e}")
                logger.warning(f"Will retry after 5 seconds...")
                time.sleep(5)
                # Try once more before giving up
                try:
                    response = httpx.get(f"{self.base_url}/events", params=params, timeout=30.0)
                    response.raise_for_status()
                    events = response.json()
                    if events:
                        num_events = len(events)
                        total_events_processed += num_events
                        markets_before = len(markets)
                        for event in events:
                            event_markets = event.get("markets", [])
                            markets.extend(event_markets)
                        markets_added = len(markets) - markets_before
                        logger.info(f"✓ Retry successful! Added {markets_added} markets")
                        offset += limit
                        page += 1
                        continue
                except Exception:
                    logger.error(f"Retry failed. Stopping pagination.")
                    break
                break
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error on page {page}: Status {e.response.status_code}")
                logger.error(f"Response: {e.response.text[:200]}")
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit! Waiting 10 seconds before retrying...")
                    time.sleep(10)
                    continue
                logger.error(f"Stopping pagination due to HTTP error")
                break
            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {type(e).__name__}: {e}", exc_info=True)
                logger.error(f"Stopping pagination due to unexpected error")
                break
        
        logger.info("=" * 80)
        logger.info(f"Polymarket data retrieval complete")
        logger.info(f"Total events processed: {total_events_processed}")
        logger.info(f"Total markets found: {len(markets)}")
        logger.info("=" * 80)
        
        return markets

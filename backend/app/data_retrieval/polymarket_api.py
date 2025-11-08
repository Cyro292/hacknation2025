import httpx
import logging
from typing import List, Dict, Any, Optional
import time
from datetime import datetime, timedelta

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
    
    def calculate_volatility_score(self, market: Dict[str, Any]) -> float:
        """
        Calculate a 24-hour volatility score (0-1) for a market.
        
        This uses multiple factors:
        1. Price spread from equilibrium (0.5) - more extreme prices = lower volatility
        2. Volume as a proxy for activity and price movement potential
        3. Number of outcomes (more outcomes = potentially higher volatility)
        
        Returns a score from 0 (low volatility/stable) to 1 (high volatility/unstable)
        """
        try:
            outcome_prices = market.get("outcomePrices", [])
            volume = float(market.get("volume", 0))
            
            if not outcome_prices or len(outcome_prices) == 0:
                return 0.0
            
            # Convert prices to floats
            try:
                prices = [float(p) for p in outcome_prices]
            except (ValueError, TypeError):
                return 0.0
            
            # Factor 1: Price uncertainty (distance from extremes)
            # Markets near 0.5 are most uncertain/volatile
            # Markets near 0.0 or 1.0 are more certain/stable
            if len(prices) == 2:
                # Binary market: measure distance from extremes (0 or 1)
                primary_price = prices[0]
                distance_from_extreme = min(primary_price, 1 - primary_price)
                # Map 0.5 (max uncertainty) = 1.0, 0.0 or 1.0 (certainty) = 0.0
                price_uncertainty = distance_from_extreme * 2  # Scale 0.5 to 1.0
            else:
                # Multi-outcome market: use entropy-like measure
                # More evenly distributed prices = higher uncertainty
                import math
                if sum(prices) > 0:
                    normalized_prices = [p / sum(prices) for p in prices]
                    entropy = -sum(p * math.log(p + 1e-10) for p in normalized_prices if p > 0)
                    max_entropy = math.log(len(prices))  # Maximum entropy for uniform distribution
                    price_uncertainty = entropy / max_entropy if max_entropy > 0 else 0.0
                else:
                    price_uncertainty = 0.0
            
            # Factor 2: Volume indicator (normalized logarithmically)
            # Higher volume can indicate more activity and price changes
            # Use log scale: 10k volume = 0.5, 100k = 0.65, 1M = 0.8, 10M = 1.0
            if volume > 0:
                import math
                # Normalize volume on log scale (0 to 1)
                # Assume 10M volume is max (score = 1.0)
                volume_factor = min(math.log10(volume + 1) / math.log10(10_000_000), 1.0)
            else:
                volume_factor = 0.0
            
            # Factor 3: Time to expiration (if available)
            # Markets closer to expiration with uncertain prices are more volatile
            end_date_str = market.get("endDate")
            time_factor = 0.5  # Default middle value
            if end_date_str:
                try:
                    # Parse ISO format or timestamp
                    if isinstance(end_date_str, str):
                        try:
                            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                        except:
                            end_date = datetime.fromtimestamp(int(end_date_str) / 1000)
                    else:
                        end_date = datetime.fromtimestamp(int(end_date_str) / 1000)
                    
                    now = datetime.utcnow()
                    days_until_close = (end_date - now).total_seconds() / 86400
                    
                    # Markets closing soon with uncertainty are more volatile
                    if days_until_close < 1:
                        time_factor = 0.9  # High volatility potential
                    elif days_until_close < 7:
                        time_factor = 0.7  # Medium-high
                    elif days_until_close < 30:
                        time_factor = 0.5  # Medium
                    else:
                        time_factor = 0.3  # Lower volatility for far-future markets
                except:
                    time_factor = 0.5  # Default if date parsing fails
            
            # Combine factors with weights
            # Price uncertainty: 50% (most important)
            # Volume activity: 30%
            # Time to expiration: 20%
            volatility_score = (
                price_uncertainty * 0.5 +
                volume_factor * 0.3 +
                time_factor * 0.2
            )
            
            # Ensure score is between 0 and 1
            volatility_score = max(0.0, min(1.0, volatility_score))
            
            return round(volatility_score, 4)
            
        except Exception as e:
            logger.debug(f"Error calculating volatility for market: {e}")
            return 0.0

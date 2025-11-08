import httpx
import logging
from typing import List, Dict, Any, Optional, Tuple
import time
from datetime import datetime, timedelta
import math
import asyncio
from collections import deque

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter to respect Polymarket API limits."""
    
    def __init__(self, max_requests: int, time_window: float):
        """
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds (e.g., 10 for 10 seconds)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
    
    async def acquire(self):
        """Wait if necessary to respect rate limit."""
        now = time.time()
        
        # Remove old requests outside the time window
        while self.requests and self.requests[0] < now - self.time_window:
            self.requests.popleft()
        
        # If at limit, wait
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                return await self.acquire()
        
        # Record this request
        self.requests.append(now)


class PolymarketVolatilityCalculator:
    """
    Calculate true 24-hour volatility using CLOB price history.
    Respects Polymarket rate limits.
    """
    
    def __init__(self):
        self.clob_base_url = "https://clob.polymarket.com"
        # CLOB Price History: 100 requests per 10 seconds
        self.rate_limiter = RateLimiter(max_requests=95, time_window=10.0)  # Leave 5 req buffer
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def calculate_true_volatility_24h(
        self, 
        condition_id: str,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[float], str, Dict[str, Any]]:
        """
        Calculate TRUE 24-hour volatility using historical price data.
        
        Returns:
            (volatility_score, method, metadata)
            where metadata includes: data_points, price_range, etc.
        """
        try:
            await self.rate_limiter.acquire()
            
            # Fetch 24h of price data
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            response = await self.client.get(
                f"{self.clob_base_url}/price-history",
                params={
                    "market": condition_id,
                    "interval": "1h",  # 1-hour intervals = 24 data points
                    "startTs": int(start_time.timestamp()),
                    "endTs": int(end_time.timestamp()),
                }
            )
            response.raise_for_status()
            
            data = response.json()
            history = data.get('history', [])
            
            if len(history) < 2:
                logger.debug(f"Insufficient price history for {condition_id}")
                return None, "insufficient_data", {}
            
            # Extract prices
            prices = [float(p['price']) for p in history if 'price' in p]
            
            if len(prices) < 2:
                return None, "insufficient_data", {}
            
            # Calculate log returns (financial standard)
            log_returns = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0 and prices[i] > 0:
                    log_return = math.log(prices[i] / prices[i-1])
                    log_returns.append(log_return)
            
            if not log_returns:
                return None, "no_valid_returns", {}
            
            # Calculate realized volatility (standard deviation)
            mean_return = sum(log_returns) / len(log_returns)
            variance = sum((r - mean_return) ** 2 for r in log_returns) / len(log_returns)
            volatility_raw = math.sqrt(variance)
            
            # Annualized volatility (standard financial metric)
            # sqrt(365) for daily to annual, but we want 0-1 scale
            # Typical prediction market daily volatility: 0-0.3 in log space
            # Normalize to 0-1 scale
            normalized_volatility = min(volatility_raw / 0.3, 1.0)
            
            # Calculate price range for metadata
            price_range = {
                "min": min(prices),
                "max": max(prices),
                "avg": sum(prices) / len(prices),
                "range": max(prices) - min(prices)
            }
            
            metadata = {
                "data_points": len(prices),
                "price_range": price_range,
                "raw_volatility": round(volatility_raw, 6),
                "mean_return": round(mean_return, 6)
            }
            
            logger.debug(
                f"Volatility for {condition_id}: {normalized_volatility:.4f} "
                f"(from {len(prices)} data points)"
            )
            
            return round(normalized_volatility, 4), "price_history", metadata
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"No price history available for {condition_id}")
                return None, "not_found", {}
            logger.warning(f"HTTP error fetching price history: {e}")
            return None, "http_error", {}
        except Exception as e:
            logger.error(f"Error calculating volatility for {condition_id}: {e}")
            return None, "error", {}
    
    def calculate_volatility_from_price_changes(self, market: Dict[str, Any]) -> Tuple[float, str, Dict[str, Any]]:
        """
        Calculate volatility using price changes from Polymarket.
        Priority: 24h > 7d > 30d > proxy fallback
        Uses square root of time scaling to convert longer periods to 24h equivalent.
        """
        try:
            # Priority 1: Use 24h price change (BEST!)
            one_day_change = market.get("oneDayPriceChange") or market.get("one_day_price_change")
            
            if one_day_change is not None:
                # Direct 24h data - no scaling needed
                abs_change = abs(float(one_day_change))
                volatility = min(abs_change / 0.3, 1.0)
                
                metadata = {
                    "source_period": "24h",
                    "one_day_change": one_day_change,
                    "raw_volatility": abs_change
                }
                
                logger.debug(f"Calculated volatility from 24h price change: {volatility:.4f}")
                return round(volatility, 4), "price_change_24h", metadata
            
            # Priority 2: Use 7-day price change (GOOD backup)
            one_week_change = market.get("oneWeekPriceChange") or market.get("one_week_price_change")
            
            if one_week_change is not None:
                # Scale 7-day volatility to 24h using sqrt(time) rule
                # volatility_24h ≈ volatility_7d / sqrt(7)
                abs_change = abs(float(one_week_change))
                scaled_change = abs_change / math.sqrt(7)  # Scale down from 7 days to 1 day
                volatility = min(scaled_change / 0.3, 1.0)
                
                metadata = {
                    "source_period": "7d",
                    "one_week_change": one_week_change,
                    "raw_volatility": abs_change,
                    "scaled_24h_volatility": scaled_change
                }
                
                logger.debug(f"Calculated volatility from 7d price change (scaled): {volatility:.4f}")
                return round(volatility, 4), "price_change_7d_scaled", metadata
            
            # Priority 3: Use 30-day price change (OK backup)
            one_month_change = market.get("oneMonthPriceChange") or market.get("one_month_price_change")
            
            if one_month_change is not None:
                # Scale 30-day volatility to 24h using sqrt(time) rule
                # volatility_24h ≈ volatility_30d / sqrt(30)
                abs_change = abs(float(one_month_change))
                scaled_change = abs_change / math.sqrt(30)  # Scale down from 30 days to 1 day
                volatility = min(scaled_change / 0.3, 1.0)
                
                metadata = {
                    "source_period": "30d",
                    "one_month_change": one_month_change,
                    "raw_volatility": abs_change,
                    "scaled_24h_volatility": scaled_change
                }
                
                logger.debug(f"Calculated volatility from 30d price change (scaled): {volatility:.4f}")
                return round(volatility, 4), "price_change_30d_scaled", metadata
            
            # No price change data available at all
            return None, "no_price_changes", {}
            
        except Exception as e:
            logger.error(f"Error calculating volatility from price changes: {e}")
            return None, "error", {}
    
    def calculate_proxy_volatility(self, market: Dict[str, Any]) -> Tuple[float, str, Dict[str, Any]]:
        """
        Fallback proxy volatility calculation (from original implementation).
        """
        try:
            outcome_prices = market.get("outcomePrices", [])
            if not outcome_prices:
                outcome_prices = market.get("outcome_prices", [])
            
            volume = float(market.get("volume", 0))
            
            if not outcome_prices:
                return 0.0, "proxy_no_data", {}
            
            prices = [float(p) for p in outcome_prices]
            
            # Price uncertainty calculation
            if len(prices) == 2:
                primary_price = prices[0]
                distance_from_extreme = min(primary_price, 1 - primary_price)
                price_uncertainty = distance_from_extreme * 2
            else:
                if sum(prices) > 0:
                    normalized_prices = [p / sum(prices) for p in prices]
                    entropy = -sum(p * math.log(p + 1e-10) for p in normalized_prices if p > 0)
                    max_entropy = math.log(len(prices))
                    price_uncertainty = entropy / max_entropy if max_entropy > 0 else 0.0
                else:
                    price_uncertainty = 0.0
            
            # Volume factor
            if volume > 0:
                volume_factor = min(math.log10(volume + 1) / math.log10(10_000_000), 1.0)
            else:
                volume_factor = 0.0
            
            # Time factor
            end_date_str = market.get("endDate") or market.get("end_date")
            time_factor = 0.5
            if end_date_str:
                try:
                    if isinstance(end_date_str, str):
                        try:
                            end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                        except:
                            end_date = datetime.fromtimestamp(int(end_date_str) / 1000)
                    else:
                        end_date = datetime.fromtimestamp(int(end_date_str) / 1000)
                    
                    days_until_close = (end_date - datetime.utcnow()).total_seconds() / 86400
                    
                    if days_until_close < 1:
                        time_factor = 0.9
                    elif days_until_close < 7:
                        time_factor = 0.7
                    elif days_until_close < 30:
                        time_factor = 0.5
                    else:
                        time_factor = 0.3
                except:
                    pass
            
            volatility_score = (
                price_uncertainty * 0.5 +
                volume_factor * 0.3 +
                time_factor * 0.2
            )
            
            volatility_score = max(0.0, min(1.0, volatility_score))
            
            metadata = {
                "price_uncertainty": round(price_uncertainty, 4),
                "volume_factor": round(volume_factor, 4),
                "time_factor": round(time_factor, 4)
            }
            
            return round(volatility_score, 4), "proxy", metadata
            
        except Exception as e:
            logger.error(f"Error in proxy calculation: {e}")
            return 0.0, "proxy_error", {}
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()


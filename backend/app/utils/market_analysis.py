"""
Market Analysis Utilities
Provides AI-powered analysis of market relationships and correlations
"""
from typing import Optional, Literal, Dict
from pydantic import BaseModel, Field
from app.schemas.market_schema import Market
from app.utils.openai_service import OpenAIHelper
import logging

logger = logging.getLogger(__name__)


def _calculate_expected_values(
    market1: Market,
    market2: Market,
    correlation: float
) -> tuple[Dict[str, float], str]:
    """
    DETERMINISTICALLY calculate expected values and best betting strategy.
    
    This function uses PURE MATHEMATICS to:
    1. Calculate probability of all 4 scenarios based on market prices and correlation
    2. Calculate expected value (EV) for each betting strategy
    3. Select the strategy with HIGHEST EV (no AI involved)
    
    Scenarios:
    1. Both Event 1 and Event 2 occur
    2. Event 1 occurs, Event 2 doesn't
    3. Event 2 occurs, Event 1 doesn't  
    4. Neither event occurs
    
    Strategies evaluated:
    - Bet YES on Event 1 (win if E1 occurs)
    - Bet YES on Event 2 (win if E2 occurs)
    - Bet NO on Event 1 (win if E1 doesn't occur)
    - Bet NO on Event 2 (win if E2 doesn't occur)
    - Arbitrage (bet opposite sides when correlated + mispriced)
    
    Args:
        market1: First market (with prices)
        market2: Second market (with prices)
        correlation: AI correlation score (0-1) - only used as input, not for decision-making
        
    Returns:
        Tuple of (expected_values dict, best_strategy string)
        
    Note:
        The best strategy is selected PURELY by comparing expected values.
        No AI is used in the decision - only mathematical calculations.
    """
    try:
        # Extract probabilities from market prices (first outcome typically)
        # These represent what the MARKET thinks (often assuming independence)
        market_p1 = float(market1.outcome_prices[0]) if market1.outcome_prices else 0.5
        market_p2 = float(market2.outcome_prices[0]) if market2.outcome_prices else 0.5
        
        # Calculate what probabilities SHOULD be given the correlation
        # The market often prices events as independent, but they may be dependent
        
        # For INDEPENDENT events: P(both) = P1 * P2
        p_both_independent = market_p1 * market_p2
        
        # For CORRELATED/DEPENDENT events, adjust based on correlation strength
        if correlation > 0.8:
            # Strong correlation - likely mutually exclusive or causally linked
            if market_p1 + market_p2 > 1:
                # Likely mutually exclusive (inverse correlation)
                p_both = min(market_p1, market_p2) * (1 - correlation)
            else:
                # Likely positive correlation (one causes the other)
                p_both = market_p1 * market_p2 * (1 + correlation)
        else:
            # Weaker correlation
            p_both = market_p1 * market_p2 * (1 + correlation * 0.5)
        
        # Normalize to ensure probabilities sum to 1
        p_both = max(0, min(p_both, min(market_p1, market_p2)))
        
        # Calculate scenario probabilities based on ADJUSTED correlation-aware probabilities
        p_1_only = market_p1 - p_both
        p_2_only = market_p2 - p_both
        p_neither = 1 - p_both - p_1_only - p_2_only
        
        # Ensure non-negative probabilities
        p_1_only = max(0, p_1_only)
        p_2_only = max(0, p_2_only)
        p_neither = max(0, p_neither)
        
        # Normalize if probabilities don't sum to 1 (due to correlation adjustments)
        total = p_both + p_1_only + p_2_only + p_neither
        if total > 0 and abs(total - 1.0) > 0.001:
            p_both /= total
            p_1_only /= total
            p_2_only /= total
            p_neither /= total
        
        # DETERMINISTIC EXPECTED VALUE CALCULATIONS
        # Compare our correlation-adjusted "true" probabilities against market prices
        # 
        # KEY INSIGHT: Markets often price events as if they're INDEPENDENT
        # But if events are CORRELATED, the true probabilities differ
        # This creates arbitrage opportunities!
        
        # TRUE probability that E1 occurs (correlation-adjusted)
        true_p1 = p_both + p_1_only
        true_p2 = p_both + p_2_only
        
        # Strategy 1: Bet YES on Event 1
        # Cost: market_p1, Payout if win: $1, Win probability: true_p1
        # EV = (win_prob * payout) - cost
        ev_bet_event1 = (true_p1 * 1.0) - market_p1 if market_p1 > 0 else 0
        
        # Strategy 2: Bet YES on Event 2
        ev_bet_event2 = (true_p2 * 1.0) - market_p2 if market_p2 > 0 else 0
        
        # Strategy 3: Bet NO on Event 1
        # Cost: (1-market_p1), Payout if win: $1, Win if E1 doesn't occur
        true_not_p1 = p_2_only + p_neither
        ev_bet_no_event1 = (true_not_p1 * 1.0) - (1 - market_p1) if market_p1 < 1 else 0
        
        # Strategy 4: Bet NO on Event 2
        # Cost: (1-market_p2), Win if E2 doesn't occur
        true_not_p2 = p_1_only + p_neither
        ev_bet_no_event2 = (true_not_p2 * 1.0) - (1 - market_p2) if market_p2 < 1 else 0
        
        # Strategy 5: Arbitrage - if correlation is high and prices differ significantly
        # Bet opposite sides when markets are correlated but mispriced
        ev_arbitrage = 0
        if correlation > 0.7 and abs(market_p1 - market_p2) > 0.15:
            # Strong correlation + price differential = arbitrage opportunity
            # For mutually exclusive events, betting both sides can guarantee profit
            if market_p1 > market_p2:
                # Bet NO on E1 (costs 1-market_p1), YES on E2 (costs market_p2)
                # Total cost: (1-market_p1) + market_p2 = 1 - market_p1 + market_p2
                # Payout: always $1 (one will win), so EV = 1 - cost
                ev_arbitrage = 1.0 - ((1 - market_p1) + market_p2)
            else:
                # Bet YES on E1, NO on E2
                ev_arbitrage = 1.0 - (market_p1 + (1 - market_p2))
        
        # Find the best EV across all strategies
        all_evs = [ev_bet_event1, ev_bet_event2, ev_bet_no_event1, ev_bet_no_event2, ev_arbitrage]
        best_ev = max(all_evs)
        
        expected_values = {
            # Overall best opportunity
            "best_ev": round(best_ev, 4),
            
            # Expected values for each strategy (profit per $1 wagered)
            "ev_yes_event1": round(ev_bet_event1, 4),
            "ev_yes_event2": round(ev_bet_event2, 4),
            "ev_no_event1": round(ev_bet_no_event1, 4),
            "ev_no_event2": round(ev_bet_no_event2, 4),
            "ev_arbitrage": round(ev_arbitrage, 4),
            
            # Market prices used
            "market1_price": round(market_p1, 4),
            "market2_price": round(market_p2, 4),
            "correlation_used": round(correlation, 4)
        }
        
        # DETERMINISTICALLY determine best strategy based on HIGHEST expected value
        strategies = {
            "Bet YES on Event 1": ev_bet_event1,
            "Bet YES on Event 2": ev_bet_event2,
            "Bet NO on Event 1": ev_bet_no_event1,
            "Bet NO on Event 2": ev_bet_no_event2,
            "Arbitrage (opposite sides)": ev_arbitrage
        }
        
        # Find strategy with highest EV (pure mathematics, no AI)
        best = max(strategies.items(), key=lambda x: x[1])
        best_strategy = f"{best[0]} (EV: {best[1]:+.4f} or {best[1]*100:+.2f}%)"
        
        # Add deterministic reasoning based on the numbers
        if best[1] > 0.05:
            best_strategy += f" - Strong positive edge! Expected to profit ${best[1]:.2f} per $1 wagered"
        elif best[1] > 0.01:
            best_strategy += f" - Moderate positive edge, expected to profit ${best[1]:.2f} per $1"
        elif best[1] > 0:
            best_strategy += f" - Slight positive edge"
        else:
            best_strategy += f" - No positive EV strategies found (market is efficient or overpriced)"
        
        # Add correlation insight
        if correlation > 0.8 and abs(market_p1 - market_p2) > 0.15:
            best_strategy += f" | High correlation ({correlation:.2f}) + price gap ({abs(market_p1-market_p2):.2f}) = strong arbitrage potential"
        elif correlation > 0.8:
            best_strategy += f" | High correlation ({correlation:.2f}) but similar prices - limited arbitrage"
        
        return expected_values, best_strategy
        
    except Exception as e:
        logger.warning(f"Failed to calculate expected values: {e}")
        return {
            "error": "Unable to calculate - insufficient price data"
        }, "Insufficient data for strategy recommendation"


class MarketCorrelationAnalysis(BaseModel):
    """Schema for comprehensive AI-generated market correlation analysis"""
    correlation_score: float = Field(
        ..., 
        ge=0.0, 
        le=1.0, 
        description="Correlation strength (0.0-1.0). High score = strong relationship (causation OR prevention/contradiction)"
    )
    explanation: str = Field(
        ..., 
        description="Brief explanation (2-3 sentences) of how the events relate (positive causation or inverse/prevention)"
    )
    investment_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Arbitrage opportunity score (0-1) based on price differentials and correlation. Higher = better arbitrage potential"
    )
    investment_rationale: str = Field(
        ...,
        description="Explanation of arbitrage opportunity considering price differentials and market conditions"
    )
    risk_level: Literal["low", "medium", "high"] = Field(
        ...,
        description="Risk assessment based on volatility and market conditions"
    )
    expected_values: Optional[Dict[str, float]] = Field(
        None,
        description="Expected value calculations for all 4 scenarios based on market prices and correlation"
    )
    best_strategy: Optional[str] = Field(
        None,
        description="Recommended betting strategy based on expected values"
    )


async def analyze_market_correlation(
    market1: Market,
    market2: Market,
    model: str = "gemini-2.0-flash"
) -> MarketCorrelationAnalysis:
    """
    Use AI to analyze if two markets influence each other, with arbitrage opportunity scoring.
    
    This function analyzes:
    - Correlation strength: How strongly related are the events? (causation OR prevention/contradiction)
    - Arbitrage opportunity: Based on price differentials, correlation, and volatility
    - Risk assessment: Based on volatility patterns
    
    Note: High correlation includes BOTH positive causation (Event 2 causes Event 1) 
    AND inverse relationships (Event 2 prevents/contradicts Event 1).
    
    Args:
        market1: First market (Event 1)
        market2: Second market (Event 2)
        model: AI model to use ("gemini-flash" for speed, "gemini-pro" for quality)
        
    Returns:
        MarketCorrelationAnalysis with correlation, arbitrage score (0-1), and risk level
        
    Example:
        >>> market1 = await db.get_market_by_id(123)  # "None leave cabinet in 2025"
        >>> market2 = await db.get_market_by_id(456)  # "First to leave: Scott Bessent"
        >>> analysis = await analyze_market_correlation(market1, market2)
        >>> print(f"Correlation: {analysis.correlation_score}")  # ~1.0 (mutually exclusive)
        >>> print(f"Arbitrage Score: {analysis.investment_score}")
        >>> print(f"Risk: {analysis.risk_level}")
    
    Raises:
        ValueError: If model is not a supported Gemini model
    """
    # Validate model - throw error if not supported
    VALID_MODELS = [
        "gemini-flash",
        "gemini-pro",
        "gemini-2.0-flash",
        "gemini-2.0-flash-exp",
        "gemini-2.0-flash-thinking-exp",
        "gemini-1.5-flash",
        "gemini-1.5-flash-002",
        "gemini-1.5-flash-8b",
    ]
    
    if model not in VALID_MODELS:
        raise ValueError(
            f"Invalid model '{model}'. Supported models: {', '.join(VALID_MODELS)}. "
            f"Use 'gemini-flash' for speed or 'gemini-pro' for quality."
        )
    
    # Map convenience names to actual model names
    MODEL_MAPPING = {
        "gemini-flash": "gemini-2.0-flash-exp",
        "gemini-pro": "gemini-2.0-flash-thinking-exp"
    }
    
    actual_model = MODEL_MAPPING.get(model, model)
    
    # Calculate volatility for both markets
    def calculate_volatility(market: Market) -> float:
        """Calculate average volatility from price changes"""
        changes = []
        if market.one_day_price_change is not None:
            changes.append(abs(market.one_day_price_change))
        if market.one_week_price_change is not None:
            changes.append(abs(market.one_week_price_change))
        if market.one_month_price_change is not None:
            changes.append(abs(market.one_month_price_change))
        return sum(changes) / len(changes) if changes else 0.0
    
    volatility1 = calculate_volatility(market1)
    volatility2 = calculate_volatility(market2)
    
    # Build comprehensive context for both markets
    market1_context = f"""Market 1: {market1.question}"""
    if market1.description:
        market1_context += f"\nDescription: {market1.description}"
    market1_context += f"\nOutcome Prices: {', '.join(market1.outcome_prices) if market1.outcome_prices else 'N/A'}"
    market1_context += f"\nVolume: ${market1.volume:,.2f}"
    market1_context += f"\nVolatility (avg price change): {volatility1:.2%}"
    if market1.one_day_price_change is not None:
        market1_context += f"\n24h Change: {market1.one_day_price_change:+.2%}"
    if market1.one_week_price_change is not None:
        market1_context += f"\n7d Change: {market1.one_week_price_change:+.2%}"
    
    market2_context = f"""Market 2: {market2.question}"""
    if market2.description:
        market2_context += f"\nDescription: {market2.description}"
    market2_context += f"\nOutcome Prices: {', '.join(market2.outcome_prices) if market2.outcome_prices else 'N/A'}"
    market2_context += f"\nVolume: ${market2.volume:,.2f}"
    market2_context += f"\nVolatility (avg price change): {volatility2:.2%}"
    if market2.one_day_price_change is not None:
        market2_context += f"\n24h Change: {market2.one_day_price_change:+.2%}"
    if market2.one_week_price_change is not None:
        market2_context += f"\n7d Change: {market2.one_week_price_change:+.2%}"
    
    system_message = """You are an expert analyst evaluating prediction markets for causal relationships and ARBITRAGE opportunities.

Your task has THREE parts:

1. CORRELATION SCORE (0.0-1.0): How strongly are Event 2 and Event 1 related? This includes BOTH positive causation AND negative/inverse relationships.
   - 0.0-0.3: Little/no relationship (independent events)
   - 0.4-0.6: Moderate relationship (some indirect influence)
   - 0.7-0.9: Strong relationship (direct causation OR direct prevention/contradiction)
   - 1.0: Absolute relationship (guarantees Event 1 OR makes Event 1 impossible)
   
   **IMPORTANT**: If Event 2 happening PREVENTS or CONTRADICTS Event 1 (makes it impossible), this is a STRONG correlation (0.8-1.0).
   Examples:
   - "First cabinet member to leave" vs "None leave in 2025" → Score ~1.0 (mutually exclusive, perfect inverse)
   - "Bitcoin hits $100k" ← "Bitcoin ETF approved" → Score ~0.8 (positive causation)

2. INVESTMENT SCORE (0.0-1.0): ARBITRAGE opportunity score based on:
   - **Price Differentials** (MOST IMPORTANT): Large price differences = high opportunity. Same/similar prices = LOW opportunity (0.0-0.2)
   - Correlation strength: Strong correlation + price difference = arbitrage potential
   - Volatility: Higher volatility = more price movement opportunities
   - Volume/liquidity: Sufficient volume for execution
   
   Scoring:
   - 0.8-1.0: Excellent arbitrage (strong correlation + large price differential + good volatility)
   - 0.5-0.7: Moderate arbitrage (some price differential exists)
   - 0.0-0.4: Poor/no arbitrage (similar prices, weak correlation, or unfavorable conditions)
   
   **CRITICAL**: If markets have the same or very similar prices, score MUST be low (0.0-0.2) as there's no arbitrage opportunity.

3. RISK LEVEL (low/medium/high): Based on volatility and market conditions
   - Low: Volatility < 5%, stable markets
   - Medium: Volatility 5-15%, moderate fluctuations
   - High: Volatility > 15%, highly volatile

Provide concise explanations (2-3 sentences each) for correlation and investment rationale. Focus on PRICE DIFFERENTIALS for arbitrage."""
  
    prompt = f"""{market1_context}

{market2_context}

Analyze these markets for ARBITRAGE opportunities:
1. Assess CORRELATION: How strongly related are these events? Consider:
   - Does Event 2 CAUSE Event 1? (positive correlation)
   - Does Event 2 PREVENT/CONTRADICT Event 1? (inverse correlation - still HIGH score!)
   - Are they mutually exclusive or logically related?
   
2. Rate ARBITRAGE opportunity: Focus on PRICE DIFFERENTIALS first, then correlation and volatility. If prices are the same/similar, score MUST be very low (0.0-0.2).

3. Assess risk level: Based on volatility patterns

**CRITICAL**: If Event 2 makes Event 1 impossible (contradiction/prevention), correlation score should be HIGH (0.8-1.0), not low!

Provide correlation_score, explanation, investment_score, investment_rationale, and risk_level."""
    
    # Create AI helper with selected model
    openai_helper = OpenAIHelper(chat_model=actual_model)
    
    # Get AI analysis
    analysis = await openai_helper.get_structured_output(
        prompt=prompt,
        response_model=MarketCorrelationAnalysis,
        system_message=system_message
    )
    
    # Calculate expected values for all 4 scenarios
    expected_values, best_strategy = _calculate_expected_values(
        market1, 
        market2, 
        analysis.correlation_score
    )
    
    # Add expected value calculations to analysis
    analysis.expected_values = expected_values
    analysis.best_strategy = best_strategy
    
    return analysis


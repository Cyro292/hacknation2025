"""
Market Analysis Utilities
Provides AI-powered analysis of market relationships and correlations
"""
from typing import Optional, Literal, Dict, Any
from pydantic import BaseModel, Field
from app.schemas.market_schema import Market
from app.utils.openai_service import OpenAIHelper
import logging
import math

logger = logging.getLogger(__name__)


def _calculate_expected_values(
    market1: Market,
    market2: Market,
    correlation: float,
    recommended_position_market1: Optional[str] = None,
    recommended_position_market2: Optional[str] = None,
    estimated_prob_market1: Optional[float] = None,
    estimated_prob_market2: Optional[float] = None,
) -> tuple[Dict[str, Any], str]:
    """
    Args:
        market1: First market (with prices)
        market2: Second market (with prices)
        correlation: Correlation score (0-1) affecting scenario probabilities
        recommended_position_market1: AI suggested position for market1 (YES/NO/AVOID)
        recommended_position_market2: AI suggested position for market2 (YES/NO/AVOID)
        estimated_prob_market1: AI estimated true probability for market1 YES outcome (0-1)
        estimated_prob_market2: AI estimated true probability for market2 YES outcome (0-1)
        
    Returns:
        Tuple of (expected_values dict, strategy_summary string)
    """
    try:
        # Extract market prices (these represent the market's probability estimates)
        def _safe_float(value, default):
            try:
                if value is None:
                    return float(default)
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        price_market1 = max(0.0, min(1.0, _safe_float(
            market1.outcome_prices[0] if market1.outcome_prices else 0.5, 0.5
        )))
        price_market2 = max(0.0, min(1.0, _safe_float(
            market2.outcome_prices[0] if market2.outcome_prices else 0.5, 0.5
        )))

        true_prob_market1 = max(0.0, min(1.0, _safe_float(
            estimated_prob_market1, price_market1
        )))
        true_prob_market2 = max(0.0, min(1.0, _safe_float(
            estimated_prob_market2, price_market2
        )))

        # Default to AVOID when recommendations are missing
        position_market1 = (recommended_position_market1 or "AVOID").upper()
        position_market2 = (recommended_position_market2 or "AVOID").upper()

        valid_positions = {"YES", "NO", "AVOID"}
        if position_market1 not in valid_positions:
            position_market1 = "AVOID"
        if position_market2 not in valid_positions:
            position_market2 = "AVOID"

        def _position_cost(position: str, price: float) -> float:
            if position == "YES":
                return price
            if position == "NO":
                return 1.0 - price
            return 0.0

        def _position_profit(position: str, price: float, outcome_yes: bool) -> float:
            if position == "YES":
                return (1.0 - price) if outcome_yes else -price
            if position == "NO":
                cost = 1.0 - price
                return price if not outcome_yes else -cost
            return 0.0

        stake_market1 = _position_cost(position_market1, price_market1)
        stake_market2 = _position_cost(position_market2, price_market2)
        total_stake = stake_market1 + stake_market2

        # If there is nothing to stake, short-circuit
        if total_stake == 0:
            expected_values = {
                "total_expected_profit": 0.0,
                "expected_roi": 0.0,
                "total_stake": 0.0,
                "market1_ev": 0.0,
                "market2_ev": 0.0,
                "scenario_probabilities": {
                    "both_yes": 0.0,
                    "market1_yes_market2_no": 0.0,
                    "market1_no_market2_yes": 0.0,
                    "both_no": 0.0,
                },
                "scenario_profits": {
                    "both_yes": 0.0,
                    "market1_yes_market2_no": 0.0,
                    "market1_no_market2_yes": 0.0,
                    "both_no": 0.0,
                },
                "best_case_profit": 0.0,
                "worst_case_profit": 0.0,
                "signed_correlation": 0.0,
                "true_prob_market1": true_prob_market1,
                "true_prob_market2": true_prob_market2,
                "price_market1": price_market1,
                "price_market2": price_market2,
            }
            strategy = "No actionable strategy - both markets flagged as AVOID."
            return expected_values, strategy

        # Determine the correlation direction based on positions (opposite bets imply inverse relationship)
        direction = 0.0
        if position_market1 != "AVOID" and position_market2 != "AVOID":
            direction = 1.0 if position_market1 == position_market2 else -1.0
        elif position_market1 != "AVOID" or position_market2 != "AVOID":
            direction = 0.0

        signed_corr = max(-0.95, min(0.95, correlation * direction))

        def _joint_probabilities(p_a: float, p_b: float, rho: float) -> Dict[str, float]:
            eps = 1e-6
            p_a = min(1 - eps, max(eps, p_a))
            p_b = min(1 - eps, max(eps, p_b))
            rho = max(-0.95, min(0.95, rho))

            root_term = math.sqrt(max(p_a * (1 - p_a) * p_b * (1 - p_b), 0.0))
            p11 = p_a * p_b + rho * root_term

            min_p11 = max(0.0, p_a + p_b - 1.0)
            max_p11 = min(p_a, p_b)
            p11 = min(max(p11, min_p11), max_p11)

            p10 = p_a - p11
            p01 = p_b - p11
            p00 = 1.0 - p_a - p_b + p11

            probs = {
                "both_yes": max(p11, 0.0),
                "market1_yes_market2_no": max(p10, 0.0),
                "market1_no_market2_yes": max(p01, 0.0),
                "both_no": max(p00, 0.0),
            }
            total_prob = sum(probs.values())
            if total_prob > 0:
                probs = {k: v / total_prob for k, v in probs.items()}
            else:
                # Fallback to independence if numerical issues arise
                probs = {
                    "both_yes": p_a * p_b,
                    "market1_yes_market2_no": p_a * (1 - p_b),
                    "market1_no_market2_yes": (1 - p_a) * p_b,
                    "both_no": (1 - p_a) * (1 - p_b),
                }
            return probs

        joint_probabilities = _joint_probabilities(
            true_prob_market1, true_prob_market2, signed_corr
        )

        scenario_profits = {
            "both_yes": _position_profit(position_market1, price_market1, True)
            + _position_profit(position_market2, price_market2, True),
            "market1_yes_market2_no": _position_profit(position_market1, price_market1, True)
            + _position_profit(position_market2, price_market2, False),
            "market1_no_market2_yes": _position_profit(position_market1, price_market1, False)
            + _position_profit(position_market2, price_market2, True),
            "both_no": _position_profit(position_market1, price_market1, False)
            + _position_profit(position_market2, price_market2, False),
        }

        expected_profit = sum(
            scenario_profits[scenario] * joint_probabilities[scenario]
            for scenario in scenario_profits
        )

        market1_ev = (
            true_prob_market1 - price_market1
            if position_market1 == "YES"
            else price_market1 - true_prob_market1
            if position_market1 == "NO"
            else 0.0
        )
        market2_ev = (
            true_prob_market2 - price_market2
            if position_market2 == "YES"
            else price_market2 - true_prob_market2
            if position_market2 == "NO"
            else 0.0
        )

        best_case_profit = max(scenario_profits.values())
        worst_case_profit = min(scenario_profits.values())
        expected_roi = expected_profit / total_stake if total_stake else 0.0

        expected_values = {
            "total_expected_profit": expected_profit,
            "expected_roi": expected_roi,
            "total_stake": total_stake,
            "market1_ev": market1_ev,
            "market2_ev": market2_ev,
            "scenario_probabilities": joint_probabilities,
            "scenario_profits": scenario_profits,
            "best_case_profit": best_case_profit,
            "worst_case_profit": worst_case_profit,
            "signed_correlation": signed_corr,
            "true_prob_market1": true_prob_market1,
            "true_prob_market2": true_prob_market2,
            "price_market1": price_market1,
            "price_market2": price_market2,
            "position_market1": position_market1,
            "position_market2": position_market2,
        }

        strategy_parts = []
        if position_market1 != "AVOID":
            strategy_parts.append(f"{position_market1} on Market 1")
        if position_market2 != "AVOID":
            strategy_parts.append(f"{position_market2} on Market 2")

        if not strategy_parts:
            best_strategy = "Hold cash – no favorable trades identified."
        else:
            actions = " & ".join(strategy_parts)
            best_strategy = (
                f"{actions}. Expected profit ≈ ${expected_profit:.2f} on ${total_stake:.2f} staked "
                f"(ROI {expected_roi * 100:.1f}%). "
                f"Best/Worst case: ${best_case_profit:.2f} / ${worst_case_profit:.2f}."
            )

        return expected_values, best_strategy

    except Exception as e:
        logger.warning(f"Failed to calculate expected values: {e}")
        return {
            "error": "Unable to calculate - insufficient price data"
        }, "Insufficient data for strategy recommendation"


class MarketCorrelationAnalysisAI(BaseModel):
    """Schema for AI response - excludes calculated fields"""
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
    recommended_position_market1: Literal["YES", "NO", "AVOID"] = Field(
        ...,
        description="Recommended position for Market 1: YES (buy yes shares), NO (buy no shares), or AVOID (don't trade)"
    )
    recommended_position_market2: Literal["YES", "NO", "AVOID"] = Field(
        ...,
        description="Recommended position for Market 2: YES (buy yes shares), NO (buy no shares), or AVOID (don't trade)"
    )
    estimated_prob_market1: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="AI's independent estimate of true probability for Market 1 YES outcome (0.0-1.0). Used for EV calculation."
    )
    estimated_prob_market2: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="AI's independent estimate of true probability for Market 2 YES outcome (0.0-1.0). Used for EV calculation."
    )


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
    recommended_position_market1: Literal["YES", "NO", "AVOID"] = Field(
        ...,
        description="Recommended position for Market 1: YES (buy yes shares), NO (buy no shares), or AVOID (don't trade)"
    )
    recommended_position_market2: Literal["YES", "NO", "AVOID"] = Field(
        ...,
        description="Recommended position for Market 2: YES (buy yes shares), NO (buy no shares), or AVOID (don't trade)"
    )
    estimated_prob_market1: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="AI's independent estimate of true probability for Market 1 YES outcome (0.0-1.0). Used for EV calculation."
    )
    estimated_prob_market2: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="AI's independent estimate of true probability for Market 2 YES outcome (0.0-1.0). Used for EV calculation."
    )
    expected_values: Optional[Dict[str, Any]] = Field(
        None,
        description="Expected value calculations including losses for the combined position strategy"
    )
    best_strategy: Optional[str] = Field(
        None,
        description="Summary of recommended combined strategy with expected value (including potential losses)"
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
    
    system_message = """You are an expert analyst evaluating prediction markets for investment opportunities.

Your task has FIVE parts:

1. CORRELATION SCORE (0.0-1.0): How strongly are Event 2 and Event 1 related?
   - 0.0-0.3: Little/no relationship (independent events)
   - 0.4-0.6: Moderate relationship (some indirect influence)
   - 0.7-0.9: Strong relationship (direct causation OR direct prevention/contradiction)
   - 1.0: Absolute relationship (guarantees Event 1 OR makes Event 1 impossible)
   
   **IMPORTANT**: If Event 2 PREVENTS Event 1, this is HIGH correlation (0.8-1.0).

2. INVESTMENT SCORE (0.0-1.0): Overall opportunity score based on:
   - Price differentials and correlation
   - Volatility and volume/liquidity
   - 0.8-1.0: Excellent | 0.5-0.7: Moderate | 0.0-0.4: Poor

3. RISK LEVEL (low/medium/high): Based on volatility
   - Low: < 5% | Medium: 5-15% | High: > 15%

4. RECOMMENDED POSITION MARKET 1: YES, NO, or AVOID
   - Consider: Is Market 1 overpriced, underpriced, or fairly priced?
   - Account for correlation effects on true probability
   - BE REALISTIC: Most markets may warrant AVOID

5. RECOMMENDED POSITION MARKET 2: YES, NO, or AVOID
   - Consider: Is Market 2 overpriced, underpriced, or fairly priced?
   - Account for correlation effects on true probability
   - BE REALISTIC: Most markets may warrant AVOID

6. ESTIMATED PROBABILITY MARKET 1: Your independent estimate of true probability (0.0-1.0) that Market 1's YES outcome occurs
   - This should differ from market price if you believe the market is mispriced
   - Account for correlation effects

7. ESTIMATED PROBABILITY MARKET 2: Your independent estimate of true probability (0.0-1.0) that Market 2's YES outcome occurs
   - This should differ from market price if you believe the market is mispriced
   - Account for correlation effects

Provide concise explanations. Your probability estimates will be used to calculate expected value against market prices."""
  
    prompt = f"""{market1_context}

{market2_context}

Analyze these correlated markets and recommend a COMBINED investment strategy:

1. Assess CORRELATION between events (0.0-1.0)
2. Rate overall INVESTMENT opportunity (0.0-1.0)
3. Assess RISK level (low/medium/high)
4. Recommend position for MARKET 1: YES, NO, or AVOID
5. Recommend position for MARKET 2: YES, NO, or AVOID
6. Estimate TRUE PROBABILITY for Market 1 YES outcome (0.0-1.0)
7. Estimate TRUE PROBABILITY for Market 2 YES outcome (0.0-1.0)

**Key considerations:**
- How does the correlation affect the true probability vs market price?
- If Market 1 is priced at {market1.outcome_prices[0] if market1.outcome_prices else 0.5} but you estimate true probability differently, there's an edge
- If Market 2 is priced at {market2.outcome_prices[0] if market2.outcome_prices else 0.5} but you estimate true probability differently, there's an edge
- For mutually exclusive events, probabilities should sum to ~1.0
- BE HONEST: If market is efficiently priced (your estimate ≈ market price), recommend AVOID

Provide: correlation_score, explanation, investment_score, investment_rationale, risk_level, recommended_position_market1, recommended_position_market2, estimated_prob_market1, estimated_prob_market2"""
    
    # Create AI helper with selected model
    openai_helper = OpenAIHelper(chat_model=actual_model)
    
    # Get AI analysis (using schema without expected_values and best_strategy)
    ai_response = await openai_helper.get_structured_output(
        prompt=prompt,
        response_model=MarketCorrelationAnalysisAI,
        system_message=system_message
    )
    
    # Calculate expected value using AI's recommended positions and probability estimates
    expected_values, best_strategy = _calculate_expected_values(
        market1, 
        market2, 
        ai_response.correlation_score,
        ai_response.recommended_position_market1,
        ai_response.recommended_position_market2,
        ai_response.estimated_prob_market1,
        ai_response.estimated_prob_market2
    )
    
    # Build full analysis with calculated fields
    analysis = MarketCorrelationAnalysis(
        correlation_score=ai_response.correlation_score,
        explanation=ai_response.explanation,
        investment_score=ai_response.investment_score,
        investment_rationale=ai_response.investment_rationale,
        risk_level=ai_response.risk_level,
        recommended_position_market1=ai_response.recommended_position_market1,
        recommended_position_market2=ai_response.recommended_position_market2,
        estimated_prob_market1=ai_response.estimated_prob_market1,
        estimated_prob_market2=ai_response.estimated_prob_market2,
        expected_values=expected_values,
        best_strategy=best_strategy
    )
    
    return analysis


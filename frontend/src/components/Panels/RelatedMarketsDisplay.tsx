// ABOUTME: Component for displaying enriched related markets
// ABOUTME: Shows detailed information about markets related to the selected market

"use client";

import { useMemo, useState } from "react";
import type { EnrichedRelatedMarket } from "@/lib/api/types";

interface RelatedMarketsDisplayProps {
  /** Array of enriched related markets to display */
  relatedMarkets: EnrichedRelatedMarket[];
  /** Whether data is currently loading */
  isLoading: boolean;
}

/**
 * RelatedMarketsDisplay component shows a list of related markets with expanded details.
 * Displays comprehensive information about each related market including metrics,
 * outcomes, volume, volatility, and AI analysis if available.
 * Sorted by AI correlation score (highest first).
 */
export function RelatedMarketsDisplay({
  relatedMarkets,
  isLoading,
}: RelatedMarketsDisplayProps) {
  // Sort by AI correlation score (highest first), then by similarity as fallback
  const sortedMarkets = useMemo(() => {
    return [...relatedMarkets].sort((a, b) => {
      const scoreA = a.ai_correlation_score ?? 0;
      const scoreB = b.ai_correlation_score ?? 0;
      if (scoreA !== scoreB) {
        return scoreB - scoreA; // Descending order
      }
      // Fallback to similarity if AI scores are equal
      return (b.similarity ?? 0) - (a.similarity ?? 0);
    });
  }, [relatedMarkets]);

  if (isLoading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "24px",
          color: "#94a3b8",
          fontSize: "14px",
        }}
      >
        <span style={{ marginRight: "8px" }}>Loading related markets...</span>
        <div
          style={{
            width: "16px",
            height: "16px",
            border: "2px solid rgba(148, 163, 184, 0.3)",
            borderTopColor: "#60a5fa",
            borderRadius: "50%",
            animation: "spin 1s linear infinite",
          }}
        />
        <style>{`
          @keyframes spin {
            to { transform: rotate(360deg); }
          }
        `}</style>
      </div>
    );
  }

  if (relatedMarkets.length === 0) {
    return (
      <div
        style={{
          color: "#64748b",
          fontSize: "14px",
          padding: "12px",
          textAlign: "center",
        }}
      >
        No related markets found
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "16px",
        marginTop: "8px",
      }}
      role="list"
      aria-label="Related markets"
    >
      {sortedMarkets.map((relatedMarket) => (
        <RelatedMarketCard key={relatedMarket.market_id} relatedMarket={relatedMarket} />
      ))}
    </div>
  );
}

/**
 * RelatedMarketCard displays detailed information about a single related market.
 */
function RelatedMarketCard({
  relatedMarket,
}: {
  relatedMarket: EnrichedRelatedMarket;
}) {
  const {
    market,
    similarity,
    correlation,
    pressure,
    ai_correlation_score,
    ai_explanation,
    expected_values,
    best_strategy,
    investment_score,
    risk_level,
  } = relatedMarket;

  // Determine pressure level
  const pressureLevel = pressure !== undefined && pressure !== null
    ? pressure > 0.7
      ? "high"
      : pressure < 0.3
      ? "low"
      : "medium"
    : null;

  // Remove ROI from best_strategy text
  const cleanStrategy = best_strategy
    ? best_strategy.replace(/\s*\(ROI\s+[^)]+\)\.?\s*/gi, " ").trim()
    : null;

  return (
    <div
      role="listitem"
      style={{
        padding: "16px",
        backgroundColor: "rgba(30, 41, 59, 0.4)",
        border: "1px solid rgba(148, 163, 184, 0.2)",
        borderRadius: "12px",
        transition: "all 150ms ease-in-out",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = "rgba(30, 41, 59, 0.6)";
        e.currentTarget.style.borderColor = "rgba(59, 130, 246, 0.3)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = "rgba(30, 41, 59, 0.4)";
        e.currentTarget.style.borderColor = "rgba(148, 163, 184, 0.2)";
      }}
    >
      {/* Market Question - Header */}
      <div
        style={{
          color: "#f1f5f9",
          fontSize: "16px",
          fontWeight: "600",
          marginBottom: "12px",
          wordBreak: "break-word",
          lineHeight: "1.4",
        }}
      >
        {market.question}
      </div>

      {/* Description if available */}
      {market.description && (
        <TruncatedText
          text={market.description}
          maxLength={200}
          style={{
            color: "#94a3b8",
            fontSize: "13px",
            marginBottom: "12px",
            lineHeight: "1.5",
            wordBreak: "break-word",
          }}
        />
      )}

      {/* Metrics Section */}
      <div
        style={{
          display: "flex",
          gap: "12px",
          marginBottom: "12px",
          flexWrap: "wrap",
          padding: "8px",
          backgroundColor: "rgba(15, 23, 42, 0.3)",
          borderRadius: "8px",
        }}
      >
        {ai_correlation_score !== null && ai_correlation_score !== undefined && (
          <MetricBadge
            label="AI Correlation"
            value={ai_correlation_score}
            highlight={true}
          />
        )}
        <MetricBadge label="Similarity" value={similarity} />
        {pressure !== undefined && (
          <MetricBadge
            label="Pressure"
            value={pressure}
            pressureLevel={pressureLevel}
          />
        )}
        {investment_score !== null && investment_score !== undefined && (
          <MetricBadge label="Investment Score" value={investment_score} />
        )}
      </div>

      {/* Expected Values - Only Total Expected Profit */}
      {expected_values && expected_values.total_expected_profit !== undefined && (
        <div style={{ marginBottom: "12px" }}>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "10px 12px",
              backgroundColor: "rgba(15, 23, 42, 0.3)",
              border: "1px solid rgba(148, 163, 184, 0.2)",
              borderRadius: "8px",
            }}
          >
            <div>
              <div
                style={{
                  color: "#94a3b8",
                  fontSize: "11px",
                  fontWeight: "600",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                  marginBottom: "4px",
                }}
              >
                Total Expected Profit
              </div>
            </div>
            <div
              style={{
                color:
                  expected_values.total_expected_profit >= 0
                    ? "#4ade80"
                    : "#f87171",
                fontSize: "16px",
                fontWeight: "700",
                minWidth: "70px",
                textAlign: "right",
              }}
            >
              {expected_values.total_expected_profit >= 0 ? "+" : ""}
              ${expected_values.total_expected_profit.toFixed(2)}
            </div>
          </div>
        </div>
      )}

      {/* Outcomes Section */}
      {market.outcomes && market.outcomes.length > 0 && (
        <div style={{ marginBottom: "12px" }}>
          <FieldLabel>Outcomes</FieldLabel>
          <OutcomesList
            outcomes={market.outcomes}
            prices={market.outcome_prices}
          />
        </div>
      )}

      {/* Market Stats Row */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: "16px",
          marginBottom: "12px",
          flexWrap: "wrap",
        }}
      >
        <StatItem label="Volume" value={`$${formatVolumeShort(market.volume)}`} />
        {market.volatility_24h !== null && market.volatility_24h !== undefined && (
          <StatItem
            label="Volatility"
            value={`${(market.volatility_24h * 100).toFixed(0)}%`}
          />
        )}
        <StatItem label="Market ID" value={market.id.toString()} />
      </div>

      {/* Tags if available */}
      {market.tags && market.tags.length > 0 && (
        <div style={{ marginBottom: "12px" }}>
          <FieldLabel>Tags</FieldLabel>
          <TagsDisplay tags={market.tags} />
        </div>
      )}

      {/* AI Explanation if available */}
      {ai_explanation && (
        <div
          style={{
            marginTop: "12px",
            padding: "12px",
            backgroundColor: "rgba(59, 130, 246, 0.1)",
            border: "1px solid rgba(59, 130, 246, 0.2)",
            borderRadius: "8px",
          }}
        >
          <FieldLabel>AI Analysis</FieldLabel>
          <TruncatedText
            text={ai_explanation}
            maxLength={200}
            style={{
              color: "#cbd5e1",
              fontSize: "13px",
              lineHeight: "1.5",
              marginTop: "4px",
              wordBreak: "break-word",
            }}
          />
        </div>
      )}

      {/* Polymarket ID */}
      {market.polymarket_id && (
        <div
          style={{
            marginTop: "8px",
            fontSize: "11px",
            color: "#64748b",
            fontFamily: "monospace",
          }}
        >
          ID: {market.polymarket_id}
        </div>
      )}

      {/* AI Suggestion / Best Strategy - At the bottom */}
      {cleanStrategy && (
        <div
          style={{
            marginTop: "12px",
            padding: "10px 12px",
            backgroundColor: "rgba(34, 197, 94, 0.1)",
            border: "1px solid rgba(34, 197, 94, 0.3)",
            borderRadius: "8px",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              marginBottom: "4px",
            }}
          >
            <span style={{ fontSize: "16px" }}>💡</span>
            <FieldLabel>AI Suggestion</FieldLabel>
          </div>
          <TruncatedText
            text={cleanStrategy}
            maxLength={200}
            style={{
              color: "#4ade80",
              fontSize: "14px",
              fontWeight: "600",
              marginTop: "4px",
              wordBreak: "break-word",
            }}
          />
        </div>
      )}
    </div>
  );
}

/**
 * MetricBadge displays a metric with label and value as a percentage.
 */
function MetricBadge({
  label,
  value,
  highlight,
  pressureLevel,
}: {
  label: string;
  value: number;
  highlight?: boolean;
  pressureLevel?: "low" | "medium" | "high" | null;
}) {
  let valueColor = "#60a5fa";
  let pressureIndicator = null;

  if (highlight) {
    valueColor = "#fbbf24"; // Yellow/gold for AI correlation
  }

  if (pressureLevel && label === "Pressure") {
    if (pressureLevel === "high") {
      valueColor = "#ef4444"; // Red for high pressure
      pressureIndicator = "↑";
    } else if (pressureLevel === "low") {
      valueColor = "#22c55e"; // Green for low pressure
      pressureIndicator = "↓";
    }
  }

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "6px",
        fontSize: "12px",
        color: "#94a3b8",
      }}
    >
      <span style={{ fontWeight: "500" }}>{label}:</span>
      <span style={{ color: valueColor, fontWeight: "600" }}>
        {(value * 100).toFixed(0)}%
        {pressureIndicator && (
          <span style={{ marginLeft: "4px", fontSize: "14px" }}>
            {pressureIndicator}
          </span>
        )}
      </span>
    </div>
  );
}

/**
 * ExpectedValuesDisplay shows expected value calculations for different scenarios.
 */
function ExpectedValuesDisplay({
  expectedValues,
}: {
  expectedValues: Record<string, any>;
}) {
  // Extract scenario names and their EV values
  const scenarios = Object.entries(expectedValues)
    .filter(([key]) => 
      key !== "best_scenario" && 
      key !== "worst_scenario" &&
      key !== "expected_roi" && // Skip ROI calculation
      !key.startsWith("_") // Skip internal/metadata keys
    )
    .map(([key, value]) => {
      let ev: number | null = null;
      
      // Handle different data structures
      if (typeof value === "number") {
        ev = value;
      } else if (typeof value === "object" && value !== null) {
        // Try common field names
        if ("ev" in value && typeof value.ev === "number") {
          ev = value.ev;
        } else if ("expected_value" in value && typeof value.expected_value === "number") {
          ev = value.expected_value;
        } else if ("value" in value && typeof value.value === "number") {
          ev = value.value;
        }
      }
      
      return {
        name: key
          .replace(/_/g, " ")
          .replace(/\b\w/g, (l) => l.toUpperCase())
          .trim(),
        ev,
      };
    })
    .filter((scenario) => scenario.ev !== null && !isNaN(scenario.ev as number));

  if (scenarios.length === 0) {
    return (
      <div
        style={{
          color: "#64748b",
          fontSize: "12px",
          padding: "8px",
          fontStyle: "italic",
        }}
      >
        No expected value data available
      </div>
    );
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "6px",
        marginTop: "6px",
      }}
    >
      {scenarios.map((scenario) => {
        const ev = scenario.ev as number;
        const isPositive = ev > 0;
        const backgroundColor = isPositive
          ? "rgba(34, 197, 94, 0.1)"
          : "rgba(239, 68, 68, 0.1)";
        const borderColor = isPositive
          ? "rgba(34, 197, 94, 0.3)"
          : "rgba(239, 68, 68, 0.3)";
        const textColor = isPositive ? "#4ade80" : "#f87171";

        return (
          <div
            key={scenario.name}
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "8px 10px",
              backgroundColor,
              border: `1px solid ${borderColor}`,
              borderRadius: "6px",
            }}
          >
            <span
              style={{
                color: "#f1f5f9",
                fontSize: "12px",
                fontWeight: "500",
              }}
            >
              {scenario.name}
            </span>
            <span
              style={{
                color: textColor,
                fontSize: "13px",
                fontWeight: "600",
                minWidth: "70px",
                textAlign: "right",
              }}
            >
              {ev >= 0 ? "+" : ""}
              {ev.toFixed(3)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/**
 * OutcomesList displays all outcomes with their prices.
 */
function OutcomesList({
  outcomes,
  prices,
}: {
  outcomes: string[];
  prices: string[];
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "6px",
        marginTop: "6px",
      }}
      role="list"
      aria-label="Market outcomes"
    >
      {outcomes.map((outcome, index) => {
        const price = prices[index] || "0";
        const priceNum = parseFloat(price);
        const isHighProbability = priceNum >= 0.5;
        const backgroundColor = isHighProbability
          ? "rgba(34, 197, 94, 0.1)"
          : "rgba(239, 68, 68, 0.1)";
        const borderColor = isHighProbability
          ? "rgba(34, 197, 94, 0.3)"
          : "rgba(239, 68, 68, 0.3)";
        const textColor = isHighProbability ? "#4ade80" : "#f87171";

        return (
          <div
            key={`${outcome}-${index}`}
            role="listitem"
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "8px 10px",
              backgroundColor,
              border: `1px solid ${borderColor}`,
              borderRadius: "6px",
              transition: "all 150ms ease-in-out",
            }}
          >
            <span
              style={{
                color: "#f1f5f9",
                fontSize: "13px",
                fontWeight: "500",
              }}
            >
              {outcome}
            </span>
            <span
              style={{
                color: textColor,
                fontSize: "14px",
                fontWeight: "600",
                minWidth: "60px",
                textAlign: "right",
              }}
            >
              ${priceNum.toFixed(3)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/**
 * TagsDisplay renders tags as accessible pill-shaped badges.
 */
function TagsDisplay({ tags }: { tags: string[] }) {
  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: "6px",
        marginTop: "6px",
      }}
      role="list"
      aria-label="Tags"
    >
      {tags.map((tag, index) => (
        <span
          key={`${tag}-${index}`}
          role="listitem"
          style={{
            display: "inline-flex",
            alignItems: "center",
            padding: "4px 10px",
            backgroundColor: "rgba(59, 130, 246, 0.15)",
            border: "1px solid rgba(59, 130, 246, 0.3)",
            borderRadius: "8px",
            color: "#60a5fa",
            fontSize: "11px",
            fontWeight: "500",
            letterSpacing: "0.025em",
            transition: "all 150ms ease-in-out",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = "rgba(59, 130, 246, 0.25)";
            e.currentTarget.style.borderColor = "rgba(59, 130, 246, 0.5)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = "rgba(59, 130, 246, 0.15)";
            e.currentTarget.style.borderColor = "rgba(59, 130, 246, 0.3)";
          }}
        >
          {tag}
        </span>
      ))}
    </div>
  );
}

/**
 * StatItem displays a statistic with label and value.
 */
function StatItem({ label, value }: { label: string; value: string }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "2px",
      }}
    >
      <span
        style={{
          fontSize: "11px",
          color: "#64748b",
          textTransform: "uppercase",
          letterSpacing: "0.05em",
        }}
      >
        {label}
      </span>
      <span
        style={{
          fontSize: "14px",
          color: "#f1f5f9",
          fontWeight: "600",
        }}
      >
        {value}
      </span>
    </div>
  );
}

/**
 * FieldLabel is a reusable component for field labels.
 */
function FieldLabel({ children }: { children: React.ReactNode }) {
  return (
    <label
      style={{
        color: "#94a3b8",
        fontSize: "12px",
        fontWeight: "600",
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        display: "block",
        marginBottom: "4px",
      }}
    >
      {children}
    </label>
  );
}

/**
 * Formats volume in a short format (K/M/B).
 */
function formatVolumeShort(val: number): string {
  if (val >= 1_000_000_000) {
    return `${(val / 1_000_000_000).toFixed(2)}B`;
  } else if (val >= 1_000_000) {
    return `${(val / 1_000_000).toFixed(2)}M`;
  } else if (val >= 1_000) {
    return `${(val / 1_000).toFixed(2)}K`;
  } else {
    return val.toFixed(0);
  }
}

/**
 * TruncatedText component displays text with truncation and expand/collapse functionality.
 * Shows first maxLength characters with a "more" button if text is longer.
 */
function TruncatedText({
  text,
  maxLength = 500,
  style,
}: {
  text: string;
  maxLength?: number;
  style?: React.CSSProperties;
}) {
  const [isExpanded, setIsExpanded] = useState(false);
  const shouldTruncate = text.length > maxLength;
  const displayText = isExpanded || !shouldTruncate ? text : text.slice(0, maxLength);

  if (!shouldTruncate) {
    return <div style={style}>{text}</div>;
  }

  return (
    <div style={style}>
      <span>
        {displayText}
        {!isExpanded && shouldTruncate && "..."}
      </span>
      {" "}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        style={{
          display: "inline",
          padding: 0,
          margin: 0,
          marginLeft: "4px",
          backgroundColor: "transparent",
          border: "none",
          color: "#64748b",
          cursor: "pointer",
          fontSize: "inherit",
          fontWeight: "400",
          textDecoration: "underline",
          textDecorationColor: "rgba(100, 116, 139, 0.4)",
          transition: "all 150ms ease-in-out",
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.color = "#94a3b8";
          e.currentTarget.style.textDecorationColor = "rgba(148, 163, 184, 0.6)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.color = "#64748b";
          e.currentTarget.style.textDecorationColor = "rgba(100, 116, 139, 0.4)";
        }}
      >
        {isExpanded ? "show less" : "show more"}
      </button>
    </div>
  );
}


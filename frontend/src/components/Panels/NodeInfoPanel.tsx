// ABOUTME: Node info panel component for displaying node details and list
// ABOUTME: Shows selected node properties or scrollable list when no selection

"use client";

import { useState } from "react";
import type { GraphNode } from "@/types/graph";
import { getNodeColor } from "@/lib/d3-helpers";
import { formatTimestamp } from "@/lib/utils";
import { usePanelState } from "@/hooks/usePanelState";

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
  const displayText =
    isExpanded || !shouldTruncate ? text : text.slice(0, maxLength);

  if (!shouldTruncate) {
    return <div style={style}>{text}</div>;
  }

  return (
    <div style={style}>
      <span>
        {displayText}
        {!isExpanded && shouldTruncate && "..."}
      </span>{" "}
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
          e.currentTarget.style.textDecorationColor =
            "rgba(148, 163, 184, 0.6)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.color = "#64748b";
          e.currentTarget.style.textDecorationColor =
            "rgba(100, 116, 139, 0.4)";
        }}
      >
        {isExpanded ? "show less" : "show more"}
      </button>
    </div>
  );
}

interface NodeInfoPanelProps {
  /** All nodes in the graph for list view */
  nodes: GraphNode[];

  /** Currently selected node (null if none selected) */
  selectedNode: GraphNode | null;

  /** Callback when user clicks a node in list view */
  onNodeSelect: (nodeId: string) => void;

  /** Callback to open the market panel for a specific market_id */
  onOpenSidebar?: (marketId: number) => void;
}

/**
 * NodeInfoPanel displays either detailed information about a selected node
 * or a scrollable list of all nodes in the graph.
 *
 * Features:
 * - Detail View: Shows name, group, volatility bar, and last update timestamp
 * - List View: Shows scrollable list of all nodes with hover effects
 * - Collapsible with arrow button on right edge
 * - Semi-transparent background with backdrop blur
 * - Smooth transitions between views
 */
export function NodeInfoPanel({
  nodes,
  selectedNode,
  onNodeSelect,
  onOpenSidebar,
}: NodeInfoPanelProps) {
  const { isCollapsed, toggleCollapse } = usePanelState("node-info", true);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);

  return (
    <div
      style={{
        position: "fixed",
        left: "24px",
        top: "24px",
        zIndex: 10,
        width: isCollapsed ? "auto" : "min(370px, calc(100vw - 48px))",
        maxHeight: "calc(100vh - 48px)",
        transition: "width 300ms ease-in-out",
      }}
    >
      <div
        style={{
          backgroundColor: "rgba(15, 23, 42, 0.8)",
          backdropFilter: "blur(12px)",
          borderRadius: "12px",
          border: "1px solid rgba(148, 163, 184, 0.2)",
          boxShadow:
            "0 4px 6px -1px rgba(0, 0, 0, 0.3), 0 2px 4px -1px rgba(0, 0, 0, 0.2)",
          padding: isCollapsed ? "12px" : "16px",
          display: "flex",
          flexDirection: "column",
          gap: "12px",
          transition: "padding 300ms ease-in-out",
        }}
      >
        {/* Header with collapse button */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: "12px",
          }}
        >
          {!isCollapsed && (
            <h2
              style={{
                color: "#f1f5f9",
                fontSize: "18px",
                fontWeight: "600",
                margin: 0,
              }}
            >
              {selectedNode ? "Node Info" : "All Nodes"}
            </h2>
          )}

          {/* Collapse toggle button */}
          <button
            onClick={toggleCollapse}
            aria-label={isCollapsed ? "Expand panel" : "Collapse panel"}
            title={isCollapsed ? "Expand panel" : "Collapse panel"}
            style={{
              backgroundColor: "rgba(30, 41, 59, 0.6)",
              border: "1px solid rgba(148, 163, 184, 0.2)",
              borderRadius: "6px",
              color: "#94a3b8",
              cursor: "pointer",
              padding: "6px 10px",
              fontSize: "16px",
              transition: "all 200ms ease-in-out",
              marginLeft: isCollapsed ? 0 : "auto",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = "rgba(59, 130, 246, 0.5)";
              e.currentTarget.style.color = "#60a5fa";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = "rgba(148, 163, 184, 0.2)";
              e.currentTarget.style.color = "#94a3b8";
            }}
          >
            {isCollapsed ? "→" : "←"}
          </button>
        </div>

        {/* Content - only show when not collapsed */}
        {!isCollapsed && (
          <>
            {selectedNode ? (
              <NodeDetailView
                node={selectedNode}
                onOpenSidebar={onOpenSidebar}
              />
            ) : (
              <NodeListView
                nodes={nodes}
                onNodeSelect={onNodeSelect}
                hoveredNodeId={hoveredNodeId}
                onHoverChange={setHoveredNodeId}
              />
            )}
          </>
        )}
      </div>
    </div>
  );
}

/**
 * NodeDetailView displays detailed information about a selected node.
 * Shows name, description, volume, outcomes, tags, volatility, and last update.
 */
function NodeDetailView({
  node,
  onOpenSidebar,
}: {
  node: GraphNode;
  onOpenSidebar?: (marketId: number) => void;
}) {
  const handleOpenSidebar = () => {
    if (node.marketId && onOpenSidebar) {
      onOpenSidebar(node.marketId);
    }
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "16px",
        maxHeight: "calc(100vh - 180px)",
        overflowY: "auto",
        paddingRight: "4px",
      }}
    >
      {/* Open Market Panel Button - only show if marketId exists */}
      {node.marketId && onOpenSidebar && (
        <button
          onClick={handleOpenSidebar}
          style={{
            backgroundColor: "rgba(59, 130, 246, 0.15)",
            border: "1px solid rgba(59, 130, 246, 0.3)",
            borderRadius: "8px",
            color: "#60a5fa",
            cursor: "pointer",
            padding: "10px 16px",
            fontSize: "14px",
            fontWeight: "500",
            transition: "all 150ms ease-in-out",
            width: "100%",
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
          Advanced Generative AI Analysis →
        </button>
      )}
      {/* Name field - show full name in detail view */}
      <FieldDisplay label="Name" value={node.fullName || node.name} />

      {/* Description field - only show if available */}
      {node.description && (
        <div>
          <FieldLabel>Description</FieldLabel>
          <TruncatedText
            text={node.description}
            maxLength={200}
            style={{
              color: "#f1f5f9",
              fontSize: "14px",
              wordBreak: "break-word",
              marginTop: "4px",
            }}
          />
        </div>
      )}

      {/* Volume field */}
      <div>
        <FieldLabel>Volume</FieldLabel>
        <VolumeDisplay volume={node.volume} />
      </div>

      {/* Outcomes comparison */}
      {node.outcomes && node.outcomes.length > 0 && (
        <div>
          <FieldLabel>Outcomes</FieldLabel>
          <OutcomesComparison
            outcomes={node.outcomes}
            prices={node.outcomePrices}
          />
        </div>
      )}

      {/* Tags field - only show if tags exist */}
      {node.tags && node.tags.length > 0 && (
        <div>
          <FieldLabel>Tags</FieldLabel>
          <TagsDisplay tags={node.tags} />
        </div>
      )}

      {/* Volatility field with gradient bar */}
      <div>
        <FieldLabel>Volatility</FieldLabel>
        <VolatilityBar volatility={node.volatility} />
      </div>

      {/* Last Update field */}
      <FieldDisplay
        label="Last Update"
        value={formatTimestamp(node.lastUpdate)}
      />

      {/* ID field at bottom for reference */}
      <FieldDisplay label="ID" value={node.id} />
    </div>
  );
}

/**
 * NodeListView displays a scrollable list of all nodes in the graph.
 * Each item shows the node ID, name, and a volatility color indicator.
 */
function NodeListView({
  nodes,
  onNodeSelect,
  hoveredNodeId,
  onHoverChange,
}: {
  nodes: GraphNode[];
  onNodeSelect: (nodeId: string) => void;
  hoveredNodeId: string | null;
  onHoverChange: (nodeId: string | null) => void;
}) {
  return (
    <div
      style={{
        maxHeight: "calc(100vh - 180px)",
        overflowY: "auto",
        display: "flex",
        flexDirection: "column",
        gap: "4px",
      }}
    >
      {nodes.length === 0 ? (
        <div
          style={{
            color: "#64748b",
            fontSize: "14px",
            textAlign: "center",
            padding: "24px 12px",
          }}
        >
          No nodes available
        </div>
      ) : (
        nodes.map((node) => (
          <div
            key={node.id}
            onClick={() => onNodeSelect(node.id)}
            onMouseEnter={() => onHoverChange(node.id)}
            onMouseLeave={() => onHoverChange(null)}
            role="button"
            tabIndex={0}
            aria-label={`Select node ${node.id}`}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onNodeSelect(node.id);
              }
            }}
            style={{
              padding: "12px",
              borderRadius: "8px",
              backgroundColor:
                hoveredNodeId === node.id
                  ? "rgba(59, 130, 246, 0.2)"
                  : "transparent",
              cursor: "pointer",
              transition: "all 150ms ease-in-out",
              display: "flex",
              alignItems: "center",
              gap: "12px",
            }}
          >
            {/* Volatility color indicator */}
            <div
              style={{
                width: "8px",
                height: "8px",
                borderRadius: "50%",
                backgroundColor: getNodeColor(node.volatility),
                flexShrink: 0,
              }}
              aria-hidden="true"
            />

            {/* Node info */}
            <div style={{ flex: 1, minWidth: 0 }}>
              <div
                style={{
                  color: "#f1f5f9",
                  fontSize: "14px",
                  fontWeight: "600",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
              >
                {node.shortened_name || node.id}
              </div>
            </div>

            {/* Group badge */}
            <div
              style={{
                color: "#64748b",
                fontSize: "11px",
                backgroundColor: "rgba(30, 41, 59, 0.6)",
                padding: "2px 8px",
                borderRadius: "4px",
                flexShrink: 0,
              }}
            >
              G{node.group}
            </div>
          </div>
        ))
      )}
    </div>
  );
}

/**
 * FieldDisplay is a reusable component for displaying a label and value pair.
 */
function FieldDisplay({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <FieldLabel>{label}</FieldLabel>
      <div
        style={{
          color: "#f1f5f9",
          fontSize: "14px",
          wordBreak: "break-word",
        }}
      >
        {value}
      </div>
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
 * Interpolates between two RGB colors based on a ratio.
 * Used to calculate the appropriate color for a given volatility level.
 *
 * @param color1 - Starting color in hex format (e.g., "#3b82f6")
 * @param color2 - Ending color in hex format (e.g., "#ef4444")
 * @param ratio - Interpolation ratio between 0 and 1
 * @returns Interpolated color in hex format
 */
function interpolateColor(
  color1: string,
  color2: string,
  ratio: number
): string {
  const hex = (color: string) => {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(color);
    return result
      ? {
          r: parseInt(result[1], 16),
          g: parseInt(result[2], 16),
          b: parseInt(result[3], 16),
        }
      : { r: 0, g: 0, b: 0 };
  };

  const c1 = hex(color1);
  const c2 = hex(color2);

  const r = Math.round(c1.r + (c2.r - c1.r) * ratio);
  const g = Math.round(c1.g + (c2.g - c1.g) * ratio);
  const b = Math.round(c1.b + (c2.b - c1.b) * ratio);

  return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
}

/**
 * VolatilityBar displays a gradient progress bar showing volatility level.
 * The gradient transitions from blue (low volatility) through yellow/orange to red (high volatility).
 * The gradient only extends to the current volatility level, not the full spectrum.
 */
function VolatilityBar({ volatility }: { volatility: number }) {
  // Calculate the color at the current volatility level
  // Blue (#3b82f6) at 0% -> Red (#ef4444) at 100%
  const endColor = interpolateColor("#3b82f6", "#ef4444", volatility);

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "8px",
      }}
    >
      {/* Progress bar container */}
      <div
        style={{
          flex: 1,
          height: "8px",
          backgroundColor: "rgba(30, 41, 59, 0.6)",
          borderRadius: "4px",
          overflow: "hidden",
        }}
      >
        {/* Progress bar fill with gradient from blue to volatility-appropriate color */}
        <div
          style={{
            width: `${volatility * 100}%`,
            height: "100%",
            background: `linear-gradient(to right, #3b82f6, ${endColor})`,
            transition: "width 300ms ease-in-out",
          }}
        />
      </div>

      {/* Percentage display */}
      <span
        style={{
          color: "#f1f5f9",
          fontSize: "12px",
          minWidth: "40px",
          textAlign: "right",
        }}
      >
        {(volatility * 100).toFixed(0)}%
      </span>
    </div>
  );
}

/**
 * TagsDisplay renders tags as accessible pill-shaped badges.
 * Follows modern design principles with proper spacing, colors, and accessibility.
 */
function TagsDisplay({ tags }: { tags: string[] }) {
  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: "6px",
        marginTop: "4px",
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
            padding: "4px 12px",
            backgroundColor: "rgba(59, 130, 246, 0.15)",
            border: "1px solid rgba(59, 130, 246, 0.3)",
            borderRadius: "12px",
            color: "#60a5fa",
            fontSize: "12px",
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
 * VolumeDisplay renders the market volume with proper formatting.
 * Displays large numbers with K/M/B abbreviations for readability.
 */
function VolumeDisplay({ volume }: { volume: number }) {
  const formatVolume = (val: number): string => {
    if (val >= 1_000_000_000) {
      return `$${(val / 1_000_000_000).toFixed(2)}B`;
    } else if (val >= 1_000_000) {
      return `$${(val / 1_000_000).toFixed(2)}M`;
    } else if (val >= 1_000) {
      return `$${(val / 1_000).toFixed(2)}K`;
    } else {
      return `$${val.toFixed(2)}`;
    }
  };

  return (
    <div
      style={{
        color: "#f1f5f9",
        fontSize: "16px",
        fontWeight: "600",
        marginTop: "4px",
      }}
    >
      {formatVolume(volume)}
    </div>
  );
}

/**
 * OutcomesComparison renders outcomes with their prices in a side-by-side comparison.
 * Uses gentle red/green colors to indicate different outcomes.
 */
function OutcomesComparison({
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
        gap: "8px",
        marginTop: "4px",
      }}
      role="list"
      aria-label="Market outcomes"
    >
      {outcomes.map((outcome, index) => {
        const price = prices[index] || "0";
        const priceNum = parseFloat(price);

        // Determine color based on price (higher = more likely = green, lower = less likely = red)
        const isHighProbability = priceNum >= 0.5;
        const backgroundColor = isHighProbability
          ? "rgba(34, 197, 94, 0.1)" // Gentle green
          : "rgba(239, 68, 68, 0.1)"; // Gentle red
        const borderColor = isHighProbability
          ? "rgba(34, 197, 94, 0.3)"
          : "rgba(239, 68, 68, 0.3)";
        const textColor = isHighProbability
          ? "#4ade80" // Green-400
          : "#f87171"; // Red-400

        return (
          <div
            key={`${outcome}-${index}`}
            role="listitem"
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "10px 12px",
              backgroundColor,
              border: `1px solid ${borderColor}`,
              borderRadius: "8px",
              transition: "all 150ms ease-in-out",
            }}
          >
            <span
              style={{
                color: "#f1f5f9",
                fontSize: "14px",
                fontWeight: "500",
              }}
            >
              {outcome}
            </span>
            <span
              style={{
                color: textColor,
                fontSize: "16px",
                fontWeight: "600",
                minWidth: "70px",
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

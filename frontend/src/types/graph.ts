// ABOUTME: Core type definitions for the trading network graph visualization
// ABOUTME: Extends D3 force simulation types with domain-specific properties for nodes, connections, and trades

import { SimulationNodeDatum, SimulationLinkDatum } from "d3-force";

/**
 * Represents a node in the trading network graph.
 * Extends D3's SimulationNodeDatum to work with force-directed graph physics.
 *
 * D3 will automatically add during simulation:
 * - x, y: Current position coordinates
 * - vx, vy: Velocity vectors
 * - fx, fy: Fixed position coordinates (when dragging)
 */
export interface GraphNode extends SimulationNodeDatum {
  /** Unique identifier for the node (e.g., "AAPL", "BTC", "GOLD") */
  id: string;

  /** Display name of the trading instrument */
  name: string;

  /** Group classification (0-9) for visual clustering and filtering */
  group: string;

  /**
   * Volatility level between 0 and 1
   * - Controls node color intensity
   * - Triggers pulsing animation when > 0.7
   * - 0 = stable, 1 = highly volatile
   */
  volatility: number;

  /** Timestamp of last data update in ISO 8601 format */
  lastUpdate: string;
}

/**
 * Represents a connection (edge) between two nodes in the graph.
 * Extends D3's SimulationLinkDatum to work with force simulation links.
 *
 * D3 will resolve string IDs to actual GraphNode references during initialization.
 */
export interface GraphConnection extends SimulationLinkDatum<GraphNode> {
  /**
   * Source node - can be string ID or GraphNode object
   * D3 converts string IDs to node references automatically
   */
  source: string | GraphNode;

  /**
   * Target node - can be string ID or GraphNode object
   * D3 converts string IDs to node references automatically
   */
  target: string | GraphNode;

  /**
   * Correlation strength between 0 and 1
   * - Controls line thickness in visualization
   * - 0 = no correlation, 1 = perfect correlation
   */
  correlation: number;

  /**
   * Trading pressure level between 0 and 1
   * - Controls glow effect intensity
   * - Triggers pulse animation when > 0.7
   * - Indicates market activity/momentum
   */
  pressure: number;
}

/**
 * Represents a recommended trading opportunity.
 * Displayed in the hot trades panel and linked to specific nodes.
 */
export interface HotTrade {
  /** Unique identifier for the trade recommendation */
  id: string;

  /** Human-readable description of the trade opportunity */
  title: string;

  /** Array of node IDs that are relevant to this trade */
  relatedNodes: string[];

  /**
   * Confidence level between 0 and 1
   * - Indicates strength of the trade signal
   * - Used for visual ranking in the UI
   */
  confidence: number;

  /**
   * Recommended trading action
   * - LONG: Buy/bullish position
   * - SHORT: Sell/bearish position
   * - NEUTRAL: Hold/observation
   */
  action: "LONG" | "SHORT" | "NEUTRAL";
}

/**
 * Complete graph data structure.
 * Contains all nodes, connections, and trade recommendations.
 */
export interface GraphData {
  /** Array of all nodes in the graph */
  nodes: GraphNode[];

  /** Array of all connections between nodes */
  connections: GraphConnection[];

  /** Array of recommended trading opportunities */
  hotTrades: HotTrade[];
}

/**
 * Represents a cluster of connected nodes.
 * Used for cluster highlighting and zoom-to-cluster functionality.
 */
export interface ClusterData {
  /** The central node that was selected */
  centerNode: GraphNode;

  /** All nodes directly connected to the center node */
  connectedNodes: GraphNode[];

  /** All connections within this cluster */
  connections: GraphConnection[];
}

/**
 * Type guard to check if an object is a valid GraphNode.
 * Useful for runtime validation of data from external sources.
 *
 * @param obj - Object to check
 * @returns True if obj is a valid GraphNode
 */
export function isGraphNode(obj: unknown): obj is GraphNode {
  if (typeof obj !== "object" || obj === null) return false;

  const node = obj as Record<string, unknown>;

  return (
    typeof node.id === "string" &&
    typeof node.name === "string" &&
    typeof node.group === "string" &&
    typeof node.volatility === "number" &&
    node.volatility >= 0 &&
    node.volatility <= 1 &&
    typeof node.lastUpdate === "string"
  );
}

/**
 * Type guard to check if an object is a valid GraphConnection.
 * Useful for runtime validation of data from external sources.
 *
 * @param obj - Object to check
 * @returns True if obj is a valid GraphConnection
 */
export function isGraphConnection(obj: unknown): obj is GraphConnection {
  if (typeof obj !== "object" || obj === null) return false;

  const conn = obj as Record<string, unknown>;

  const hasValidSource =
    typeof conn.source === "string" || isGraphNode(conn.source);
  const hasValidTarget =
    typeof conn.target === "string" || isGraphNode(conn.target);

  return (
    hasValidSource &&
    hasValidTarget &&
    typeof conn.correlation === "number" &&
    conn.correlation >= 0 &&
    conn.correlation <= 1 &&
    typeof conn.pressure === "number" &&
    conn.pressure >= 0 &&
    conn.pressure <= 1
  );
}

/**
 * Type guard to check if an object is a valid HotTrade.
 * Useful for runtime validation of data from external sources.
 *
 * @param obj - Object to check
 * @returns True if obj is a valid HotTrade
 */
export function isHotTrade(obj: unknown): obj is HotTrade {
  if (typeof obj !== "object" || obj === null) return false;

  const trade = obj as Record<string, unknown>;

  return (
    typeof trade.id === "string" &&
    typeof trade.title === "string" &&
    Array.isArray(trade.relatedNodes) &&
    trade.relatedNodes.every((node) => typeof node === "string") &&
    typeof trade.confidence === "number" &&
    trade.confidence >= 0 &&
    trade.confidence <= 1 &&
    (trade.action === "LONG" ||
      trade.action === "SHORT" ||
      trade.action === "NEUTRAL")
  );
}

/**
 * Type guard to check if an object is a valid GraphData structure.
 * Validates the complete graph data including all nodes, connections, and trades.
 *
 * @param obj - Object to check
 * @returns True if obj is a valid GraphData structure
 */
export function isGraphData(obj: unknown): obj is GraphData {
  if (typeof obj !== "object" || obj === null) return false;

  const data = obj as Record<string, unknown>;

  return (
    Array.isArray(data.nodes) &&
    data.nodes.every(isGraphNode) &&
    Array.isArray(data.connections) &&
    data.connections.every(isGraphConnection) &&
    Array.isArray(data.hotTrades) &&
    data.hotTrades.every(isHotTrade)
  );
}

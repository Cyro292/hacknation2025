// ABOUTME: Mock data generator for trading network graph with realistic trading instruments
// ABOUTME: Provides seeded random generation, factory functions, and data validation for consistent test data

import {
  GraphNode,
  GraphConnection,
  HotTrade,
  GraphData,
  isGraphNode,
  isGraphConnection,
  isHotTrade,
} from "@/types/graph";

/**
 * Configuration options for graph data generation
 */
export interface GeneratorConfig {
  /** Number of nodes to generate */
  nodeCount: number;

  /**
   * Average number of connections per node
   * Actual connections will vary randomly around this value
   */
  avgConnectionsPerNode: number;

  /** Number of hot trades to generate */
  hotTradeCount: number;

  /** Random seed for reproducible generation */
  seed?: number;
}

/**
 * Default configuration for generating a 100-node graph
 */
const DEFAULT_CONFIG: GeneratorConfig = {
  nodeCount: 100,
  avgConnectionsPerNode: 2.5,
  hotTradeCount: 15,
  seed: 42,
};

/**
 * Seeded pseudo-random number generator using mulberry32 algorithm.
 * Provides reproducible random numbers for consistent test data.
 *
 * @param seed - Seed value for the generator
 * @returns Function that generates random numbers between 0 and 1
 */
function createSeededRandom(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Trading instrument categories with realistic names
 */
const TRADING_INSTRUMENTS = {
  stocks: [
    "AAPL",
    "GOOGL",
    "MSFT",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "JPM",
    "V",
    "JNJ",
    "WMT",
    "PG",
    "MA",
    "UNH",
    "HD",
    "DIS",
    "BAC",
    "ADBE",
    "CRM",
    "NFLX",
    "INTC",
    "CSCO",
    "PFE",
    "KO",
    "PEP",
    "TMO",
    "ABT",
    "MRK",
    "ABBV",
    "NKE",
  ],
  crypto: [
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "ADA",
    "XRP",
    "DOT",
    "DOGE",
    "AVAX",
    "MATIC",
    "LINK",
    "UNI",
    "ATOM",
    "LTC",
    "NEAR",
    "ALGO",
    "XLM",
    "VET",
    "ICP",
    "FIL",
  ],
  commodities: [
    "GOLD",
    "SILVER",
    "OIL",
    "NATGAS",
    "COPPER",
    "PLATINUM",
    "WHEAT",
    "CORN",
    "SOYBEAN",
    "COFFEE",
    "SUGAR",
    "COTTON",
    "LUMBER",
    "PALLADIUM",
    "CRUDE",
  ],
  forex: [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "AUD/USD",
    "USD/CAD",
    "NZD/USD",
    "EUR/GBP",
    "EUR/JPY",
    "GBP/JPY",
  ],
  indices: [
    "SPX",
    "NDX",
    "DJI",
    "RUT",
    "VIX",
    "DAX",
    "FTSE",
    "NIKKEI",
    "HSI",
    "CAC",
  ],
};

/**
 * All trading instruments flattened into a single array
 */
const ALL_INSTRUMENTS = [
  ...TRADING_INSTRUMENTS.stocks,
  ...TRADING_INSTRUMENTS.crypto,
  ...TRADING_INSTRUMENTS.commodities,
  ...TRADING_INSTRUMENTS.forex,
  ...TRADING_INSTRUMENTS.indices,
];

/**
 * Generates a human-readable name for a trading instrument
 */
const INSTRUMENT_NAMES: Record<string, string> = {
  // Stocks
  AAPL: "Apple Inc.",
  GOOGL: "Alphabet Inc.",
  MSFT: "Microsoft Corp.",
  AMZN: "Amazon.com Inc.",
  META: "Meta Platforms",
  TSLA: "Tesla Inc.",
  NVDA: "NVIDIA Corp.",
  JPM: "JPMorgan Chase",
  V: "Visa Inc.",
  JNJ: "Johnson & Johnson",
  WMT: "Walmart Inc.",
  PG: "Procter & Gamble",
  MA: "Mastercard Inc.",
  UNH: "UnitedHealth Group",
  HD: "Home Depot",
  DIS: "Walt Disney Co.",
  BAC: "Bank of America",
  ADBE: "Adobe Inc.",
  CRM: "Salesforce Inc.",
  NFLX: "Netflix Inc.",
  INTC: "Intel Corp.",
  CSCO: "Cisco Systems",
  PFE: "Pfizer Inc.",
  KO: "Coca-Cola Co.",
  PEP: "PepsiCo Inc.",
  TMO: "Thermo Fisher",
  ABT: "Abbott Labs",
  MRK: "Merck & Co.",
  ABBV: "AbbVie Inc.",
  NKE: "Nike Inc.",
  // Crypto
  BTC: "Bitcoin",
  ETH: "Ethereum",
  BNB: "Binance Coin",
  SOL: "Solana",
  ADA: "Cardano",
  XRP: "Ripple",
  DOT: "Polkadot",
  DOGE: "Dogecoin",
  AVAX: "Avalanche",
  MATIC: "Polygon",
  LINK: "Chainlink",
  UNI: "Uniswap",
  ATOM: "Cosmos",
  LTC: "Litecoin",
  NEAR: "Near Protocol",
  ALGO: "Algorand",
  XLM: "Stellar",
  VET: "VeChain",
  ICP: "Internet Computer",
  FIL: "Filecoin",
  // Commodities
  GOLD: "Gold Futures",
  SILVER: "Silver Futures",
  OIL: "Crude Oil",
  NATGAS: "Natural Gas",
  COPPER: "Copper Futures",
  PLATINUM: "Platinum",
  WHEAT: "Wheat Futures",
  CORN: "Corn Futures",
  SOYBEAN: "Soybean Futures",
  COFFEE: "Coffee Futures",
  SUGAR: "Sugar Futures",
  COTTON: "Cotton Futures",
  LUMBER: "Lumber Futures",
  PALLADIUM: "Palladium",
  CRUDE: "WTI Crude",
  // Forex
  "EUR/USD": "Euro/US Dollar",
  "GBP/USD": "British Pound/USD",
  "USD/JPY": "US Dollar/Yen",
  "USD/CHF": "US Dollar/Franc",
  "AUD/USD": "Aussie Dollar/USD",
  "USD/CAD": "US Dollar/CAD",
  "NZD/USD": "NZ Dollar/USD",
  "EUR/GBP": "Euro/Pound",
  "EUR/JPY": "Euro/Yen",
  "GBP/JPY": "Pound/Yen",
  // Indices
  SPX: "S&P 500 Index",
  NDX: "Nasdaq 100",
  DJI: "Dow Jones",
  RUT: "Russell 2000",
  VIX: "Volatility Index",
  DAX: "DAX Index",
  FTSE: "FTSE 100",
  NIKKEI: "Nikkei 225",
  HSI: "Hang Seng",
  CAC: "CAC 40",
};

/**
 * Trade action templates for generating realistic trade recommendations
 */
const TRADE_TEMPLATES = {
  LONG: [
    "Bullish breakout signal",
    "Strong upward momentum detected",
    "Positive correlation cluster forming",
    "Buy signal on technical indicators",
    "Accumulation phase identified",
    "Breakout above resistance",
    "Golden cross formation",
    "Strong buying pressure",
  ],
  SHORT: [
    "Bearish reversal pattern",
    "Downward momentum accelerating",
    "Negative correlation signal",
    "Sell signal on technical indicators",
    "Distribution phase detected",
    "Break below support",
    "Death cross formation",
    "Strong selling pressure",
  ],
  NEUTRAL: [
    "Consolidation phase",
    "Ranging market conditions",
    "Mixed signals across timeframes",
    "Waiting for confirmation",
    "Sideways momentum",
    "Neutral technical setup",
    "Monitor for breakout",
    "Hold current positions",
  ],
};

/**
 * Factory function to create a trading node with realistic data
 *
 * @param index - Node index for ID generation
 * @param random - Seeded random function
 * @returns A complete GraphNode object
 */
function createNode(index: number, random: () => number): GraphNode {
  const instrumentId = ALL_INSTRUMENTS[index % ALL_INSTRUMENTS.length];
  const name = INSTRUMENT_NAMES[instrumentId] || instrumentId;

  // Distribute groups evenly across 0-9
  const group = String(index % 10);

  // Generate volatility with bias toward moderate values (0.3-0.7)
  // Using beta-like distribution for realistic volatility
  const rawVolatility = (random() + random() + random()) / 3;
  const volatility = Math.round(rawVolatility * 100) / 100;

  // Random timestamp within the last 24 hours
  const now = new Date();
  const minutesAgo = Math.floor(random() * 1440); // 0-1440 minutes (24 hours)
  const lastUpdate = new Date(
    now.getTime() - minutesAgo * 60 * 1000
  ).toISOString();

  return {
    id: instrumentId,
    name,
    group,
    volatility,
    lastUpdate,
  };
}

/**
 * Factory function to create a connection between two nodes
 *
 * @param sourceId - Source node ID
 * @param targetId - Target node ID
 * @param random - Seeded random function
 * @returns A complete GraphConnection object
 */
function createConnection(
  sourceId: string,
  targetId: string,
  random: () => number
): GraphConnection {
  // Correlation tends toward higher values for visible connections
  // Using skewed distribution: favor 0.4-0.9 range
  const rawCorrelation = Math.pow(random(), 0.7);
  const correlation = Math.round(rawCorrelation * 100) / 100;

  // Pressure varies more uniformly
  const pressure = Math.round(random() * 100) / 100;

  return {
    source: sourceId,
    target: targetId,
    correlation,
    pressure,
  };
}

/**
 * Factory function to create a hot trade recommendation
 *
 * @param index - Trade index for ID generation
 * @param availableNodeIds - Array of valid node IDs to reference
 * @param random - Seeded random function
 * @returns A complete HotTrade object
 */
function createHotTrade(
  index: number,
  availableNodeIds: string[],
  random: () => number
): HotTrade {
  const actions: Array<"LONG" | "SHORT" | "NEUTRAL"> = [
    "LONG",
    "SHORT",
    "NEUTRAL",
  ];
  const action = actions[Math.floor(random() * actions.length)];

  // Select template for trade title
  const templates = TRADE_TEMPLATES[action];
  const title = templates[Math.floor(random() * templates.length)];

  // Select 1-4 related nodes
  const relatedCount = Math.floor(random() * 3) + 1; // 1-3 nodes
  const relatedNodes: string[] = [];
  const shuffled = [...availableNodeIds].sort(() => random() - 0.5);

  for (let i = 0; i < Math.min(relatedCount, shuffled.length); i++) {
    relatedNodes.push(shuffled[i]);
  }

  // Confidence skewed toward higher values (0.5-0.95)
  const rawConfidence = 0.5 + random() * 0.45;
  const confidence = Math.round(rawConfidence * 100) / 100;

  return {
    id: `trade-${index}`,
    title,
    relatedNodes,
    confidence,
    action,
  };
}

/**
 * Main function to generate complete graph data with nodes, connections, and trades
 *
 * @param config - Configuration options (uses defaults if not provided)
 * @returns Complete GraphData object with validated structure
 */
export function generateGraphData(
  config: Partial<GeneratorConfig> = {}
): GraphData {
  const finalConfig: GeneratorConfig = { ...DEFAULT_CONFIG, ...config };
  const random = createSeededRandom(finalConfig.seed || 42);

  // Generate nodes
  const nodes: GraphNode[] = [];
  for (let i = 0; i < finalConfig.nodeCount; i++) {
    nodes.push(createNode(i, random));
  }

  // Build node ID lookup for validation
  const nodeIds = new Set(nodes.map((n) => n.id));

  // Generate connections
  const connections: GraphConnection[] = [];
  const targetConnectionCount = Math.floor(
    finalConfig.nodeCount * finalConfig.avgConnectionsPerNode
  );

  // Track connections to avoid duplicates
  const connectionSet = new Set<string>();

  while (connections.length < targetConnectionCount) {
    const sourceIdx = Math.floor(random() * nodes.length);
    const targetIdx = Math.floor(random() * nodes.length);

    // Skip self-connections
    if (sourceIdx === targetIdx) continue;

    const sourceId = nodes[sourceIdx].id;
    const targetId = nodes[targetIdx].id;

    // Create bidirectional connection key to avoid duplicates
    const connKey = [sourceId, targetId].sort().join("--");

    if (!connectionSet.has(connKey)) {
      connectionSet.add(connKey);
      connections.push(createConnection(sourceId, targetId, random));
    }
  }

  // Generate hot trades
  const hotTrades: HotTrade[] = [];
  for (let i = 0; i < finalConfig.hotTradeCount; i++) {
    hotTrades.push(createHotTrade(i, Array.from(nodeIds), random));
  }

  return {
    nodes,
    connections,
    hotTrades,
  };
}

/**
 * Validates that all connections reference existing nodes
 *
 * @param data - Graph data to validate
 * @returns Object with validation result and any error messages
 */
export function validateGraphData(data: GraphData): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Build node ID set for quick lookup
  const nodeIds = new Set(data.nodes.map((n) => n.id));

  // Validate all nodes
  data.nodes.forEach((node, idx) => {
    if (!isGraphNode(node)) {
      errors.push(`Invalid node at index ${idx}`);
    }
  });

  // Validate all connections reference existing nodes
  data.connections.forEach((conn, idx) => {
    if (!isGraphConnection(conn)) {
      errors.push(`Invalid connection at index ${idx}`);
      return;
    }

    const sourceId =
      typeof conn.source === "string" ? conn.source : conn.source.id;
    const targetId =
      typeof conn.target === "string" ? conn.target : conn.target.id;

    if (!nodeIds.has(sourceId)) {
      errors.push(
        `Connection ${idx}: source node "${sourceId}" does not exist`
      );
    }
    if (!nodeIds.has(targetId)) {
      errors.push(
        `Connection ${idx}: target node "${targetId}" does not exist`
      );
    }
  });

  // Validate all hot trades
  data.hotTrades.forEach((trade, idx) => {
    if (!isHotTrade(trade)) {
      errors.push(`Invalid hot trade at index ${idx}`);
      return;
    }

    trade.relatedNodes.forEach((nodeId) => {
      if (!nodeIds.has(nodeId)) {
        errors.push(
          `Hot trade "${trade.id}": related node "${nodeId}" does not exist`
        );
      }
    });
  });

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Validates and returns data, throwing an error if validation fails
 *
 * @param data - Graph data to validate
 * @returns The validated data
 * @throws Error if validation fails
 */
export function ensureValidGraphData(data: GraphData): GraphData {
  const validation = validateGraphData(data);

  if (!validation.valid) {
    throw new Error(
      `Graph data validation failed:\n${validation.errors.join("\n")}`
    );
  }

  return data;
}

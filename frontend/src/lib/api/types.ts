// ABOUTME: TypeScript interfaces matching the backend API specification
// ABOUTME: Provides type safety for all API requests and responses

/**
 * Backend Market model
 */
export interface Market {
  id: number;
  polymarket_id: string;
  question: string;
  description?: string | null;
  outcomes: string[];
  outcome_prices: string[];
  end_date?: string | null; // ISO 8601 datetime
  volume: number;
  is_active: boolean;
  slug?: string | null;
  one_day_price_change?: number | null;
  one_week_price_change?: number | null;
  one_month_price_change?: number | null;
  tags: string[];
  created_at: string; // ISO 8601 datetime
  updated_at: string; // ISO 8601 datetime
  last_scraped_at?: string | null; // ISO 8601 datetime
  shortened_name?: string; // Added field for shortened display name
  volatility_24h?: number | null; // 0.0-1.0 volatility score
}

/**
 * Backend MarketRelation model
 */
export interface MarketRelation {
  id: number;
  market_id_1: number;
  market_id_2: number;
  similarity: number; // 0.0-1.0
  correlation: number; // 0.0-1.0
  pressure: number; // 0.0-1.0
  created_at: string; // ISO 8601 datetime
  updated_at: string; // ISO 8601 datetime
}

/**
 * Related market result from relations endpoint
 */
export interface RelatedMarket {
  market_id: number;
  similarity: number;
  correlation?: number;
  pressure?: number;
  ai_correlation_score?: number | null;
  ai_explanation?: string | null;
  investment_score?: number | null;
  investment_rationale?: string | null;
  risk_level?: string | null;
  expected_values?: Record<string, any> | null;
  best_strategy?: string | null;
}

/**
 * Enriched related market with full market details
 */
export interface EnrichedRelatedMarket extends RelatedMarket {
  market: Market;
}

/**
 * API response for market list endpoint
 */
export interface MarketListResponse {
  markets: Market[];
  total: number;
  page: number;
  page_size: number;
}

/**
 * API response for single market endpoint
 */
export interface MarketResponse {
  market: Market;
}

/**
 * API response for similarity search
 */
export interface SimilaritySearchResponse {
  results: Array<{
    market_id: number;
    similarity: number;
  }>;
  count: number;
}

/**
 * API response for relation searches
 */
export interface RelationSearchResponse {
  source_market_id: number;
  related_markets: RelatedMarket[];
  count: number;
}

/**
 * API response for enriched relation searches
 */
export interface EnrichedRelationResponse {
  source_market_id: number;
  source_market?: Market | null;
  related_markets: EnrichedRelatedMarket[];
  count: number;
}

/**
 * Parameters for GET /api/markets/
 */
export interface GetMarketsParams {
  limit?: number; // 1-1000, default 100
  offset?: number; // >= 0, default 0
  is_active?: boolean | null;
  order_by?: string; // default "created_at"
  ascending?: boolean; // default false
}

/**
 * Parameters for GET /api/markets/search/query
 */
export interface SearchMarketsParams {
  q: string; // Search query
  limit?: number; // 1-100, default 20
}

/**
 * Parameters for GET /api/relations/{market_id}
 */
export interface GetRelationsParams {
  limit?: number; // 1-1000, default 10
  min_similarity?: number; // 0.0-1.0, default 0.7
  min_volume?: number | null;
  ai_analysis?: boolean; // default false
}

/**
 * Parameters for GET /api/relations/{market_id}/enriched
 */
export interface GetEnrichedRelationsParams extends GetRelationsParams {
  ai_model?: string; // AI model: 'gemini-flash' (fast) or 'gemini-pro' (quality), default 'gemini-flash'
}

/**
 * Graph node from /api/relations/graph endpoint
 */
export interface GraphNodeResponse {
  id: string; // polymarket_id
  name: string; // Full market question
  shortened_name?: string; // Shortened display name
  group: string; // Category/tag group
  volatility?: number | null; // 0.0-1.0
  volume: number;
  lastUpdate: string; // ISO 8601 datetime
  market_id: number; // Database ID for reference
}

/**
 * Graph connection from /api/relations/graph endpoint
 */
export interface GraphConnectionResponse {
  source: string; // polymarket_id
  target: string; // polymarket_id
  correlation: number; // 0.0-1.0
  pressure: number; // 0.0-1.0
  similarity: number; // 0.0-1.0
}

/**
 * Complete graph response from /api/relations/graph endpoint
 */
export interface GraphResponse {
  nodes: GraphNodeResponse[];
  connections: GraphConnectionResponse[];
  total_nodes: number;
  total_connections: number;
}

/**
 * Parameters for GET /api/relations/graph
 */
export interface GetGraphParams {
  limit?: number; // 1-500, default 100
  min_similarity?: number; // 0.0-1.0, default 0.7
  is_active?: boolean | null; // default true
}

/**
 * Validation error response from API
 */
export interface HTTPValidationError {
  detail: Array<{
    loc: Array<string | number>;
    msg: string;
    type: string;
  }>;
}

/**
 * Type guard to check if response is a validation error
 */
export function isHTTPValidationError(obj: unknown): obj is HTTPValidationError {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    'detail' in obj &&
    Array.isArray((obj as HTTPValidationError).detail)
  );
}

/**
 * Type guard to check if object is a Market
 */
export function isMarket(obj: unknown): obj is Market {
  if (typeof obj !== 'object' || obj === null) return false;

  const market = obj as Market;
  return (
    typeof market.id === 'number' &&
    typeof market.polymarket_id === 'string' &&
    typeof market.question === 'string' &&
    Array.isArray(market.outcomes) &&
    Array.isArray(market.outcome_prices) &&
    typeof market.volume === 'number' &&
    typeof market.is_active === 'boolean' &&
    Array.isArray(market.tags) &&
    typeof market.created_at === 'string' &&
    typeof market.updated_at === 'string'
  );
}

/**
 * Type guard to check if object is a MarketRelation
 */
export function isMarketRelation(obj: unknown): obj is MarketRelation {
  if (typeof obj !== 'object' || obj === null) return false;

  const relation = obj as MarketRelation;
  return (
    typeof relation.id === 'number' &&
    typeof relation.market_id_1 === 'number' &&
    typeof relation.market_id_2 === 'number' &&
    typeof relation.similarity === 'number' &&
    typeof relation.correlation === 'number' &&
    typeof relation.pressure === 'number' &&
    typeof relation.created_at === 'string' &&
    typeof relation.updated_at === 'string'
  );
}

/**
 * Type guard to check if object is a MarketListResponse
 */
export function isMarketListResponse(obj: unknown): obj is MarketListResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const response = obj as MarketListResponse;
  return (
    Array.isArray(response.markets) &&
    response.markets.every(isMarket) &&
    typeof response.total === 'number' &&
    typeof response.page === 'number' &&
    typeof response.page_size === 'number'
  );
}

/**
 * Type guard to check if object is a RelationSearchResponse
 */
export function isRelationSearchResponse(obj: unknown): obj is RelationSearchResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const response = obj as RelationSearchResponse;
  return (
    typeof response.source_market_id === 'number' &&
    Array.isArray(response.related_markets) &&
    typeof response.count === 'number'
  );
}

/**
 * Type guard to check if object is an EnrichedRelationResponse
 */
export function isEnrichedRelationResponse(obj: unknown): obj is EnrichedRelationResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const response = obj as EnrichedRelationResponse;
  return (
    typeof response.source_market_id === 'number' &&
    Array.isArray(response.related_markets) &&
    typeof response.count === 'number' &&
    response.related_markets.every(rm =>
      typeof rm.market_id === 'number' &&
      typeof rm.similarity === 'number' &&
      isMarket(rm.market)
    )
  );
}

/**
 * Type guard to check if object is a GraphNodeResponse
 */
export function isGraphNodeResponse(obj: unknown): obj is GraphNodeResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const node = obj as GraphNodeResponse;
  return (
    typeof node.id === 'string' &&
    typeof node.name === 'string' &&
    typeof node.group === 'string' &&
    typeof node.volume === 'number' &&
    typeof node.lastUpdate === 'string' &&
    typeof node.market_id === 'number'
  );
}

/**
 * Type guard to check if object is a GraphConnectionResponse
 */
export function isGraphConnectionResponse(obj: unknown): obj is GraphConnectionResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const connection = obj as GraphConnectionResponse;
  return (
    typeof connection.source === 'string' &&
    typeof connection.target === 'string' &&
    typeof connection.correlation === 'number' &&
    typeof connection.pressure === 'number' &&
    typeof connection.similarity === 'number'
  );
}

/**
 * Type guard to check if object is a GraphResponse
 */
export function isGraphResponse(obj: unknown): obj is GraphResponse {
  if (typeof obj !== 'object' || obj === null) return false;

  const response = obj as GraphResponse;
  return (
    Array.isArray(response.nodes) &&
    response.nodes.every(isGraphNodeResponse) &&
    Array.isArray(response.connections) &&
    response.connections.every(isGraphConnectionResponse) &&
    typeof response.total_nodes === 'number' &&
    typeof response.total_connections === 'number'
  );
}
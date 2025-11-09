// ABOUTME: Relation-related API endpoint functions for fetching market relationships
// ABOUTME: Provides typed wrappers around the relations API endpoints

import { apiClient } from '../client';
import type {
  MarketRelation,
  RelationSearchResponse,
  EnrichedRelationResponse,
  GetRelationsParams,
  GetEnrichedRelationsParams,
  SimilaritySearchResponse,
  GraphResponse,
  GetGraphParams,
} from '../types';
import { isRelationSearchResponse, isEnrichedRelationResponse, isGraphResponse } from '../types';
import { ValidationError } from '../errors';

/**
 * Get related markets for a specific market (lightweight version)
 * @param marketId - Source market ID
 * @param params - Query parameters for filtering
 * @returns Promise resolving to relation search response
 */
export async function getRelatedMarkets(
  marketId: number,
  params?: GetRelationsParams
): Promise<RelationSearchResponse> {
  const response = await apiClient.get<RelationSearchResponse>(
    `/api/relations/${marketId}`,
    { params: params ? { ...params } : undefined }
  );

  // Validate response structure
  if (!isRelationSearchResponse(response)) {
    throw new ValidationError('Invalid relation search response structure');
  }

  return response;
}

/**
 * Get related markets with full market details (enriched version)
 * @param marketId - Source market ID
 * @param params - Query parameters for filtering
 * @param timeout - Optional timeout in milliseconds (default: 30s, 120s for AI analysis)
 * @returns Promise resolving to enriched relation response
 */
export async function getEnrichedRelatedMarkets(
  marketId: number,
  params?: GetEnrichedRelationsParams,
  timeout?: number
): Promise<EnrichedRelationResponse> {
  // Use longer timeout for AI analysis requests (120 seconds)
  const requestTimeout = timeout ?? (params?.ai_analysis ? 120000 : 30000);
  
  const response = await apiClient.get<EnrichedRelationResponse>(
    `/api/relations/${marketId}/enriched`,
    { 
      params: params ? { ...params } : undefined,
      timeout: requestTimeout,
    }
  );

  // Validate response structure
  if (!isEnrichedRelationResponse(response)) {
    throw new ValidationError('Invalid enriched relation response structure');
  }

  return response;
}

/**
 * Get relation between two specific markets
 * @param marketId1 - First market ID
 * @param marketId2 - Second market ID
 * @returns Promise resolving to market relation
 */
export async function getRelationBetweenMarkets(
  marketId1: number,
  marketId2: number
): Promise<MarketRelation> {
  return apiClient.get<MarketRelation>(
    `/api/relations/between/${marketId1}/${marketId2}`
  );
}

/**
 * Find markets similar to a specific market
 * @param marketId - Reference market ID
 * @param limit - Maximum number of results (default 10)
 * @returns Promise resolving to similarity search response
 */
export async function findSimilarToMarket(
  marketId: number,
  limit: number = 10
): Promise<SimilaritySearchResponse> {
  return apiClient.get<SimilaritySearchResponse>(
    `/api/vectors/search/similar-to-market/${marketId}`,
    { params: { limit } }
  );
}

/**
 * Find markets similar to a text query
 * @param query - Search query text
 * @param limit - Maximum number of results (default 10)
 * @returns Promise resolving to similarity search response
 */
export async function findSimilarToText(
  query: string,
  limit: number = 10
): Promise<SimilaritySearchResponse> {
  return apiClient.get<SimilaritySearchResponse>(
    '/api/vectors/search/similar-to-text',
    { params: { q: query, limit } }
  );
}

/**
 * Find all markets within proximity to a specific market
 * @param marketId - Reference market ID
 * @param threshold - Minimum similarity score (0.0-1.0, default 0.7)
 * @returns Promise resolving to similarity search response
 */
export async function findMarketsInProximityToMarket(
  marketId: number,
  threshold: number = 0.7
): Promise<SimilaritySearchResponse> {
  return apiClient.get<SimilaritySearchResponse>(
    `/api/vectors/search/proximity-to-market/${marketId}`,
    { params: { threshold } }
  );
}

/**
 * Find all markets within proximity to a text query
 * @param query - Search query text
 * @param threshold - Minimum similarity score (0.0-1.0, default 0.7)
 * @returns Promise resolving to similarity search response
 */
export async function findMarketsInProximityToText(
  query: string,
  threshold: number = 0.7
): Promise<SimilaritySearchResponse> {
  return apiClient.get<SimilaritySearchResponse>(
    '/api/vectors/search/proximity-to-text',
    { params: { q: query, threshold } }
  );
}

/**
 * Get relation statistics for a market
 * @param marketId - Market ID
 * @returns Promise resolving to relation statistics
 */
export async function getRelationStatistics(marketId: number): Promise<unknown> {
  return apiClient.get(`/api/relations/statistics/${marketId}`);
}

/**
 * Count relations in database
 * @param marketId - Optional market ID to count relations for
 * @returns Promise resolving to count object
 */
export async function countRelations(marketId?: number): Promise<{ count: number }> {
  return apiClient.get('/api/relations/count', {
    params: marketId ? { market_id: marketId } : undefined
  });
}

/**
 * Helper function to fetch all relations for multiple markets
 * @param marketIds - Array of market IDs
 * @param params - Query parameters for each relation fetch
 * @returns Promise resolving to map of market ID to relations
 */
export async function getAllRelationsForMarkets(
  marketIds: number[],
  params?: GetRelationsParams
): Promise<Map<number, RelationSearchResponse>> {
  const relationsMap = new Map<number, RelationSearchResponse>();

  // Fetch relations in parallel with concurrency limit
  const BATCH_SIZE = 10;
  for (let i = 0; i < marketIds.length; i += BATCH_SIZE) {
    const batch = marketIds.slice(i, i + BATCH_SIZE);
    const promises = batch.map(marketId =>
      getRelatedMarkets(marketId, params)
        .then(response => ({ marketId, response }))
        .catch(error => {
          console.warn(`Failed to fetch relations for market ${marketId}:`, error);
          return null;
        })
    );

    const results = await Promise.all(promises);
    results.forEach(result => {
      if (result) {
        relationsMap.set(result.marketId, result.response);
      }
    });
  }

  return relationsMap;
}

/**
 * Helper function to fetch enriched relations for multiple markets
 * @param marketIds - Array of market IDs
 * @param params - Query parameters for each relation fetch
 * @param timeout - Optional timeout in milliseconds (default: 30s, 120s for AI analysis)
 * @returns Promise resolving to map of market ID to enriched relations
 */
export async function getAllEnrichedRelationsForMarkets(
  marketIds: number[],
  params?: GetEnrichedRelationsParams,
  timeout?: number
): Promise<Map<number, EnrichedRelationResponse>> {
  const relationsMap = new Map<number, EnrichedRelationResponse>();

  // Fetch relations in parallel with concurrency limit
  const BATCH_SIZE = 5; // Smaller batch size for enriched data
  for (let i = 0; i < marketIds.length; i += BATCH_SIZE) {
    const batch = marketIds.slice(i, i + BATCH_SIZE);
    const promises = batch.map(marketId =>
      getEnrichedRelatedMarkets(marketId, params, timeout)
        .then(response => ({ marketId, response }))
        .catch(error => {
          console.warn(`Failed to fetch enriched relations for market ${marketId}:`, error);
          return null;
        })
    );

    const results = await Promise.all(promises);
    results.forEach(result => {
      if (result) {
        relationsMap.set(result.marketId, result.response);
      }
    });
  }

  return relationsMap;
}

/**
 * Get complete graph data for visualization
 * Uses the optimized /api/relations/graph endpoint
 * @param params - Query parameters
 * @returns Promise resolving to graph response
 */
export async function getGraphVisualization(
  params?: GetGraphParams
): Promise<GraphResponse> {
  const response = await apiClient.get<GraphResponse>(
    '/api/relations/graph',
    { params: params ? { ...params } : undefined }
  );

  // Validate response structure
  if (!isGraphResponse(response)) {
    throw new ValidationError('Invalid graph response structure');
  }

  return response;
}
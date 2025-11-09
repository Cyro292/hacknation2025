// ABOUTME: Market panel component for displaying detailed market information
// ABOUTME: Right-side panel similar to NodeInfoPanel but specifically for market details

"use client";

import { useEffect, useState, useRef } from "react";
import { getAllEnrichedRelationsForMarkets } from "@/lib/api/endpoints/relations";
import type { EnrichedRelationResponse } from "@/lib/api/types";
import { RelatedMarketsDisplay } from "./RelatedMarketsDisplay";

interface MarketPanelProps {
  /** Currently open market_id (null if panel should be closed) */
  marketId: number | null;

  /** Callback when panel is closed */
  onClose: () => void;
}

/**
 * MarketPanel displays detailed information about a specific market
 * in a panel on the right side of the screen.
 *
 * Features:
 * - Fixed position on right side
 * - Shows detailed market information
 * - Semi-transparent background with backdrop blur
 * - Responsive width
 */
export function MarketPanel({
  marketId,
  onClose,
}: MarketPanelProps) {
  // Don't render if no market is selected
  if (!marketId) {
    return null;
  }

  return (
    <div
      style={{
        position: "fixed",
        right: "24px",
        top: "24px",
        zIndex: 10,
        width: "min(320px, calc(100vw - 48px))",
        maxHeight: "calc(100vh - 48px)",
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
          padding: "16px",
          display: "flex",
          flexDirection: "column",
          gap: "12px",
        }}
      >
        {/* Header with close button */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: "12px",
          }}
        >
          <h2
            style={{
              color: "#f1f5f9",
              fontSize: "18px",
              fontWeight: "600",
              margin: 0,
            }}
          >
            Market Details
          </h2>

          {/* Close button */}
          <button
            onClick={onClose}
            aria-label="Close panel"
            title="Close panel"
            style={{
              backgroundColor: "rgba(30, 41, 59, 0.6)",
              border: "1px solid rgba(148, 163, 184, 0.2)",
              borderRadius: "6px",
              color: "#94a3b8",
              cursor: "pointer",
              padding: "6px 10px",
              fontSize: "16px",
              transition: "all 200ms ease-in-out",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = "rgba(239, 68, 68, 0.5)";
              e.currentTarget.style.color = "#f87171";
              e.currentTarget.style.backgroundColor = "rgba(239, 68, 68, 0.1)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = "rgba(148, 163, 184, 0.2)";
              e.currentTarget.style.color = "#94a3b8";
              e.currentTarget.style.backgroundColor = "rgba(30, 41, 59, 0.6)";
            }}
          >
            ✕
          </button>
        </div>

        {/* Content */}
        <MarketDetailView marketId={marketId} />
      </div>
    </div>
  );
}

/**
 * MarketDetailView fetches and displays enriched relations for the market.
 * Removed duplicate source market data - that's already shown in the node.
 */
function MarketDetailView({ marketId }: { marketId: number }) {
  const [relationsData, setRelationsData] = useState<EnrichedRelationResponse | null>(null);
  const [isLoadingRelations, setIsLoadingRelations] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const relationsCacheRef = useRef<Map<number, EnrichedRelationResponse>>(new Map());

  // Fetch enriched relations when marketId changes (if not already cached)
  useEffect(() => {
    let isMounted = true;
    let timeoutId: NodeJS.Timeout | null = null;

    // Reset error state
    setError(null);

    // Check if we already have cached data for this market
    const cached = relationsCacheRef.current.get(marketId);
    if (cached) {
      if (isMounted) {
        setRelationsData(cached);
        setIsLoadingRelations(false);
      }
      return;
    }

    // Otherwise, fetch new data with AI analysis enabled
    setIsLoadingRelations(true);
    
    // Add timeout to prevent infinite loading (130 seconds - slightly longer than API timeout)
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error("Request timeout: API call took too long"));
      }, 130000); // 130 second timeout (API timeout is 120s for AI analysis)
    });

    const fetchPromise = getAllEnrichedRelationsForMarkets([marketId], {
      ai_analysis: true,
      ai_model: "gemini-flash", // Use gemini-flash for faster analysis
      limit: 5, // Get more results for better sorting
    });

    Promise.race([fetchPromise, timeoutPromise])
      .then((relationsMap) => {
        if (!isMounted) return;
        
        const response = relationsMap.get(marketId);
        if (response) {
          setRelationsData(response);
          // Cache the response
          relationsCacheRef.current.set(marketId, response);
          setError(null);
        } else {
          setError("No data received from server");
        }
      })
      .catch((error) => {
        if (!isMounted) return;
        
        console.error("Failed to fetch enriched relations:", error);
        setError(error.message || "Failed to load related markets");
        setRelationsData(null);
      })
      .finally(() => {
        if (isMounted) {
          setIsLoadingRelations(false);
        }
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
      });

    // Cleanup function
    return () => {
      isMounted = false;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [marketId]);

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
      {/* Related Markets Section */}
      <div>
        <FieldLabel>Related Markets</FieldLabel>
        {error ? (
          <div
            style={{
              padding: "16px",
              backgroundColor: "rgba(239, 68, 68, 0.1)",
              color: "#f87171",
              fontSize: "14px",
            }}
          >
            <div style={{ fontWeight: "600", marginBottom: "4px" }}>Error loading markets</div>
            <div style={{ fontSize: "12px", color: "#fca5a5" }}>{error}</div>
            <button
              onClick={() => {
                // Clear cache and retry
                relationsCacheRef.current.delete(marketId);
                setError(null);
                setIsLoadingRelations(true);
                getAllEnrichedRelationsForMarkets([marketId], {
                  ai_analysis: true,
                  ai_model: "gemini-flash", // Use gemini-flash for faster analysis
                  limit: 3,
                })
                  .then((relationsMap) => {
                    const response = relationsMap.get(marketId);
                    if (response) {
                      setRelationsData(response);
                      relationsCacheRef.current.set(marketId, response);
                    }
                  })
                  .catch((err) => {
                    console.error("Retry failed:", err);
                    setError(err.message || "Failed to load related markets");
                  })
                  .finally(() => {
                    setIsLoadingRelations(false);
                  });
              }}
              style={{
                marginTop: "8px",
                padding: "6px 12px",
                backgroundColor: "rgba(239, 68, 68, 0.2)",
                border: "1px solid rgba(239, 68, 68, 0.4)",
                borderRadius: "6px",
                color: "#f87171",
                cursor: "pointer",
                fontSize: "12px",
                fontWeight: "500",
              }}
            >
              Retry
            </button>
          </div>
        ) : (
          <RelatedMarketsDisplay
            relatedMarkets={relationsData?.related_markets || []}
            isLoading={isLoadingRelations}
          />
        )}
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


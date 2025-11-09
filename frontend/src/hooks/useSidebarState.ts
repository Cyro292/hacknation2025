// ABOUTME: Custom hook for managing market panel open/close state with market_id association
// ABOUTME: Provides state management for a right panel that displays market details

import { useState, useCallback } from "react";

/**
 * Controller interface for market panel state management
 */
export interface MarketPanelStateController {
  /** Currently open market_id (null if panel is closed) */
  openMarketId: number | null;
  /** Whether the panel is currently open */
  isOpen: boolean;
  /** Opens the panel for a specific market_id */
  openPanel: (marketId: number) => void;
  /** Closes the panel */
  closePanel: () => void;
  /** Toggles the panel for a specific market_id (opens if closed or different market, closes if same market) */
  togglePanel: (marketId: number) => void;
}

/**
 * Custom hook for managing market panel open/close state with market_id association.
 *
 * This hook manages which market's details are displayed in the right panel.
 * The panel can be opened for a specific market_id and closed when needed.
 *
 * @returns Controller object with panel state and control functions
 *
 * @example
 * // In a component
 * const panel = useMarketPanelState();
 *
 * // Open panel for market 123
 * panel.openPanel(123);
 *
 * // Check if panel is open
 * if (panel.isOpen) {
 *   // Display market details for panel.openMarketId
 * }
 *
 * // Close panel
 * panel.closePanel();
 */
export function useMarketPanelState(): MarketPanelStateController {
  const [openMarketId, setOpenMarketId] = useState<number | null>(null);

  /**
   * Opens the panel for a specific market_id
   */
  const openPanel = useCallback((marketId: number) => {
    setOpenMarketId(marketId);
  }, []);

  /**
   * Closes the panel
   */
  const closePanel = useCallback(() => {
    setOpenMarketId(null);
  }, []);

  /**
   * Toggles the panel for a specific market_id
   * - Opens if panel is closed or if a different market_id is provided
   * - Closes if the same market_id is already open
   */
  const togglePanel = useCallback((marketId: number) => {
    setOpenMarketId((current) => (current === marketId ? null : marketId));
  }, []);

  return {
    openMarketId,
    isOpen: openMarketId !== null,
    openPanel,
    closePanel,
    togglePanel,
  };
}


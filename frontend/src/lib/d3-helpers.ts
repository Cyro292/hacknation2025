// ABOUTME: D3 utility functions for graph visualization styling and calculations
// ABOUTME: Maps data properties (volatility, correlation) to visual properties (colors, widths)

/**
 * Maps a node's volatility value (0-1) to a corresponding theme color.
 * Uses a 4-tier color scale as specified:
 * - < 0.3: very light blue (calm)
 * - 0.3-0.5: light blue
 * - 0.5-0.7: standard blue
 * - > 0.7: dark blue (highly volatile)
 *
 * @param volatility - Value between 0 and 1 representing market volatility
 * @returns CSS color string matching the theme's volatility color scale
 */
export function getNodeColor(volatility: number): string {
  if (volatility < 0.3) {
    return "#dbeafe"; // very light blue
  }
  if (volatility < 0.5) {
    return "#93c5fd"; // light blue
  }
  if (volatility < 0.7) {
    return "#3b82f6"; // standard blue
  }
  return "#1e3a8a"; // dark blue
}

/**
 * Maps a connection's correlation value (0-1) to a line thickness in pixels.
 * Higher correlation results in thicker lines to emphasize stronger relationships.
 *
 * @param correlation - Value between 0 and 1 representing connection strength
 * @returns Line width in pixels (0.5px to 5px range)
 */
export function getConnectionWidth(correlation: number): number {
  // Linear mapping: 0 -> 0.5px, 1 -> 5px
  return 0.5 + correlation * 4.5;
}

/**
 * Returns the base color for connections from the theme.
 *
 * @returns CSS color string for connection lines
 */
export function getConnectionColor(): string {
  return "rgba(148, 163, 184, 0.3)"; // connection-base from theme
}

/**
 * Calculates the radius for a node based on its volatility.
 * Higher volatility nodes are larger for clear visual emphasis.
 * Linear mapping from volatility to radius for noticeable diversity.
 *
 * @param volatility - Value between 0 and 1 representing market volatility
 * @returns Node radius in pixels (5-11px range, 6px variation)
 */
export function getNodeRadius(volatility: number): number {
  const minRadius = 5;
  const maxRadius = 11;
  const radiusRange = maxRadius - minRadius;

  // Linear mapping: 0 volatility = 5px, 1 volatility = 11px
  // This creates clear visual diversity (6px variation is very noticeable)
  return minRadius + (volatility * radiusRange);
}

/**
 * Checks if a node should have pulsing animation based on volatility.
 * Nodes with volatility > 0.7 will pulse with light green glow.
 *
 * @param volatility - Value between 0 and 1 representing market volatility
 * @returns Boolean indicating if node should pulse
 */
export function shouldNodePulse(volatility: number): boolean {
  return volatility > 0.7;
}

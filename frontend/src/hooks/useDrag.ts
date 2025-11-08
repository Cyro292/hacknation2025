// ABOUTME: Utility function for creating D3 drag behavior for graph nodes
// ABOUTME: Handles drag start, drag, and drag end events with proper physics simulation integration

import { drag, type D3DragEvent } from "d3-drag";
import type { Simulation } from "d3-force";
import type { GraphNode, GraphConnection } from "@/types/graph";

/**
 * Creates a D3 drag behavior for interactive node manipulation.
 *
 * The drag behavior implements three phases:
 * 1. **Drag Start**: Fixes the node position and reheats the simulation for responsive physics
 * 2. **Drag**: Updates the node's fixed position to follow the cursor
 * 3. **Drag End**: Releases the node and cools down the simulation
 *
 * This creates a natural feel where dragging a node causes the surrounding nodes
 * to adjust their positions in response to the new layout.
 *
 * @param simulation - The D3 force simulation instance
 * @returns D3 drag behavior that can be applied to SVG elements, or null if simulation is null
 */
export function createDragBehavior(
  simulation: Simulation<GraphNode, GraphConnection> | null
) {
  console.log("[DEBUG useDrag] createDragBehavior called with simulation:", simulation);

  if (!simulation) {
    console.warn("[DEBUG useDrag] No simulation provided, returning null");
    return null;
  }

  /**
   * Handles the start of a drag operation.
   * Sets fixed position and increases simulation energy for responsive updates.
   */
  function dragStarted(
    event: D3DragEvent<SVGCircleElement, GraphNode, GraphNode>,
    d: GraphNode
  ) {
    console.log("[DEBUG useDrag] dragStarted - node:", d.id, "event.active:", event.active);

    // Only reheat simulation if this is the only active drag
    // event.active tracks the number of concurrent drag operations
    if (!event.active && simulation) {
      simulation.alphaTarget(0.3).restart();
      console.log("[DEBUG useDrag] Simulation reheated");
    }

    // Fix the node position at its current location
    // fx and fy are D3's fixed position properties
    d.fx = d.x;
    d.fy = d.y;
    console.log("[DEBUG useDrag] Node position fixed at:", d.fx, d.fy);
  }

  /**
   * Handles the drag motion.
   * Updates the node's fixed position to the current cursor location.
   */
  function dragged(
    event: D3DragEvent<SVGCircleElement, GraphNode, GraphNode>,
    d: GraphNode
  ) {
    console.log("[DEBUG useDrag] dragged - node:", d.id, "to position:", event.x, event.y);

    // Update fixed position to cursor coordinates
    // The simulation will use these values instead of calculating new positions
    d.fx = event.x;
    d.fy = event.y;
  }

  /**
   * Handles the end of a drag operation.
   * Releases the fixed position and cools down the simulation.
   */
  function dragEnded(
    event: D3DragEvent<SVGCircleElement, GraphNode, GraphNode>,
    d: GraphNode
  ) {
    console.log("[DEBUG useDrag] dragEnded - node:", d.id, "event.active:", event.active);

    // Cool down simulation when no more active drags
    if (!event.active && simulation) {
      simulation.alphaTarget(0);
      console.log("[DEBUG useDrag] Simulation cooled down");
    }

    // Release the fixed position - node will now respond to forces again
    // Set to null (not undefined) to remove the constraint
    d.fx = null;
    d.fy = null;
    console.log("[DEBUG useDrag] Node position released");
  }

  // Create and return the drag behavior with typed event handlers
  return drag<SVGCircleElement, GraphNode>()
    .on("start", dragStarted)
    .on("drag", dragged)
    .on("end", dragEnded);
}

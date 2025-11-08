// ABOUTME: Custom React hook for managing D3 force simulation physics
// ABOUTME: Handles simulation initialization, tick updates, and cleanup for force-directed graph layout

import { useEffect, useRef, useCallback } from "react";
import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide,
  type Simulation,
} from "d3-force";
import type { GraphNode, GraphConnection } from "@/types/graph";

interface UseForceSimulationProps {
  nodes: GraphNode[];
  connections: GraphConnection[];
  width: number;
  height: number;
  onTick?: () => void;
  onSimulationCreated?: (
    simulation: Simulation<GraphNode, GraphConnection>
  ) => void;
}

/**
 * Custom hook that initializes and manages a D3 force simulation for graph layout.
 *
 * The simulation applies multiple forces to position nodes:
 * - Link force: Creates spring-like connections between related nodes
 * - Many-body force: Applies repulsion between all nodes (with distance limit for performance)
 * - Center force: Pulls the entire graph toward the center of the viewport
 * - Collision force: Prevents nodes from overlapping
 *
 * @param props - Configuration including nodes, connections, dimensions, tick callback, and simulation ready callback
 * @returns Function to get current simulation instance
 */
export function useForceSimulation({
  nodes,
  connections,
  width,
  height,
  onTick,
  onSimulationCreated,
}: UseForceSimulationProps) {
  const simulationRef = useRef<Simulation<GraphNode, GraphConnection> | null>(
    null
  );

  useEffect(() => {
    console.log("[DEBUG useForceSimulation] Effect triggered", {
      nodesCount: nodes.length,
      width,
      height
    });

    // Skip if no data provided
    if (nodes.length === 0) {
      console.log("[DEBUG useForceSimulation] No nodes, skipping");
      return;
    }

    if (width === 0 || height === 0) {
      console.log("[DEBUG useForceSimulation] No dimensions, skipping");
      return;
    }

    console.log("[DEBUG useForceSimulation] Creating simulation...");

    // D3 force simulation is designed to mutate nodes in-place, adding x, y, vx, vy properties.
    // The caller (ForceGraph) is responsible for providing mutable copies of the data.
    // React will see these changes and re-render when we call the onTick callback.

    // Initialize D3 force simulation
    const simulation = forceSimulation<GraphNode>(nodes)
      // Link force: connects nodes based on the connections array
      .force(
        "link",
        forceLink<GraphNode, GraphConnection>(connections)
          .id((d) => d.id)
          .distance(100) // Preferred distance between connected nodes
          .strength((d) => d.correlation) // Stronger correlation = stronger spring
      )
      // Many-body force: nodes repel each other
      .force(
        "charge",
        forceManyBody<GraphNode>()
          .strength(-300) // Negative = repulsion, positive = attraction
          .distanceMax(400) // Performance optimization: limit force calculation range
      )
      // Center force: pulls graph toward viewport center
      .force("center", forceCenter<GraphNode>(width / 2, height / 2))
      // Collision force: prevents node overlap
      .force(
        "collide",
        forceCollide<GraphNode>()
          .radius(12) // Slightly larger than visual node radius for spacing
          .strength(0.7)
      );

    // Handle simulation tick events
    simulation.on("tick", () => {
      if (onTick) {
        onTick();
      }
    });

    // Store simulation reference for external access
    simulationRef.current = simulation;

    console.log("[DEBUG useForceSimulation] Simulation created successfully");

    // Notify callback that simulation is ready
    if (onSimulationCreated) {
      console.log("[DEBUG useForceSimulation] Calling onSimulationCreated callback");
      onSimulationCreated(simulation);
    } else {
      console.warn("[DEBUG useForceSimulation] No onSimulationCreated callback provided");
    }

    // Cleanup function: stop simulation when component unmounts or dependencies change
    return () => {
      simulation.stop();
      simulationRef.current = null;
    };
  }, [nodes, connections, width, height, onTick, onSimulationCreated]);

  // Return a stable getter function for accessing the simulation
  const getSimulation = useCallback(
    () => simulationRef.current,
    []
  );

  return getSimulation;
}

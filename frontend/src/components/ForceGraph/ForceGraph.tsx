// ABOUTME: Main force-directed graph visualization component for displaying trading network
// ABOUTME: Renders nodes and connections using D3 force simulation with SVG, applying theme-based styling

"use client";

import { useCallback, useState, useRef, useEffect } from "react";
import { select } from "d3-selection";
import type { Simulation } from "d3-force";
import type { ZoomTransform } from "d3-zoom";
import type { GraphData, GraphNode, GraphConnection } from "@/types/graph";
import { useForceSimulation } from "@/hooks/useForceSimulation";
import { createDragBehavior } from "@/hooks/useDrag";
import { createZoomBehavior, type ZoomController } from "@/hooks/useZoom";
import {
  getNodeColor,
  getConnectionWidth,
  getConnectionColor,
  getNodeRadius,
  shouldNodePulse,
} from "@/lib/d3-helpers";

interface ForceGraphProps {
  data: GraphData;
  onZoomControllerCreated?: (controller: ZoomController) => void;
}

/**
 * ForceGraph component renders a force-directed network graph visualization.
 *
 * Features:
 * - Physics-based node positioning using D3 force simulation
 * - Nodes colored by volatility level
 * - Connections styled by correlation strength
 * - Smooth animations as the simulation stabilizes
 * - Responsive to container dimensions
 *
 * @param props - Graph data
 */
export function ForceGraph({ data, onZoomControllerCreated }: ForceGraphProps) {
  // Create a stable mutable copy of the data ONCE using useState with lazy initialization
  // D3 will mutate these objects in place (adding x, y, vx, vy properties)
  // We must use the same object references for both simulation and rendering
  // Using useState with a function ensures the copy is created only once on mount
  const [mutableData] = useState(() => {
    // Deduplicate nodes by ID to prevent React key errors
    const nodeMap = new Map<string, typeof data.nodes[0]>();
    data.nodes.forEach(node => {
      if (!nodeMap.has(node.id)) {
        nodeMap.set(node.id, { ...node });
      }
    });
    const uniqueNodes = Array.from(nodeMap.values());

    return {
      nodes: uniqueNodes,
      connections: data.connections.map((conn) => ({ ...conn })),
      hotTrades: data.hotTrades,
    };
  });

  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const transformGroupRef = useRef<SVGGElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [currentSimulation, setCurrentSimulation] = useState<Simulation<
    GraphNode,
    GraphConnection
  > | null>(null);

  // State to trigger re-renders on simulation tick
  const [, setTick] = useState(0);

  // Zoom state
  const [zoomTransform, setZoomTransform] = useState<ZoomTransform | null>(null);

  // Callback to force re-render when simulation updates node positions
  const handleTick = useCallback(() => {
    setTick((t) => t + 1);
  }, []);

  // Callback to capture simulation instance when created
  const handleSimulationCreated = useCallback(
    (sim: Simulation<GraphNode, GraphConnection>) => {
      console.log("[DEBUG] handleSimulationCreated called");
      console.log("[DEBUG] Simulation instance:", sim);
      setCurrentSimulation(sim);
    },
    []
  );

  // Measure container dimensions on mount and window resize
  useEffect(() => {
    const measureContainer = () => {
      if (containerRef.current) {
        const { width, height } = containerRef.current.getBoundingClientRect();
        setDimensions({ width, height });
      }
    };

    // Initial measurement
    measureContainer();

    // Re-measure on window resize
    window.addEventListener("resize", measureContainer);

    return () => {
      window.removeEventListener("resize", measureContainer);
    };
  }, []);

  // Initialize and manage D3 force simulation
  // Pass the mutable copies - D3 will update these in place
  useForceSimulation({
    nodes: mutableData.nodes,
    connections: mutableData.connections,
    width: dimensions.width,
    height: dimensions.height,
    onTick: handleTick,
    onSimulationCreated: handleSimulationCreated,
  });

  // Apply drag behavior to nodes AFTER they are rendered in the DOM
  // This effect runs after the nodes are painted to the screen
  useEffect(() => {
    console.log("[DEBUG] Drag application effect triggered", {
      hasSimulation: !!currentSimulation,
      hasSvg: !!svgRef.current,
      nodesCount: mutableData.nodes.length
    });

    if (!currentSimulation || !svgRef.current) {
      console.log("[DEBUG] Cannot apply drag - missing simulation or SVG ref");
      return;
    }

    const dragBehavior = createDragBehavior(currentSimulation);
    console.log("[DEBUG] Drag behavior created:", dragBehavior);

    if (!dragBehavior) {
      console.warn("[DEBUG] Failed to create drag behavior");
      return;
    }

    const svg = select(svgRef.current);
    const nodeCircles = svg.selectAll<SVGCircleElement, GraphNode>(".node-circle");

    console.log("[DEBUG] Selecting .node-circle elements");
    console.log("[DEBUG] Found", nodeCircles.size(), "node circles");
    console.log("[DEBUG] Node circle DOM elements:", nodeCircles.nodes());

    if (nodeCircles.size() === 0) {
      console.warn("[DEBUG] No .node-circle elements found in DOM!");
      // Debug: Let's see what elements ARE in the SVG
      console.log("[DEBUG] All SVG children:", svgRef.current.querySelectorAll("*"));
      return;
    }

    // CRITICAL: Bind node data to circle elements so drag handlers receive the data
    // React renders circles in mutableData.nodes order, so we bind the same array by index
    console.log("[DEBUG] Binding data to", mutableData.nodes.length, "nodes");
    nodeCircles.data(mutableData.nodes);
    console.log("[DEBUG] Data bound successfully");

    nodeCircles.call(dragBehavior);
    console.log("[DEBUG] ✅ Drag behavior successfully applied to", nodeCircles.size(), "nodes");
  }, [currentSimulation, mutableData.nodes]);

  // Apply zoom behavior to SVG AFTER dimensions are measured
  // This effect runs after the SVG is rendered with proper dimensions
  useEffect(() => {
    console.log("[DEBUG] Zoom application effect triggered", {
      hasSvg: !!svgRef.current,
      hasTransformGroup: !!transformGroupRef.current,
      width: dimensions.width,
      height: dimensions.height,
    });

    if (!svgRef.current || !transformGroupRef.current || dimensions.width === 0 || dimensions.height === 0) {
      console.log("[DEBUG] Cannot apply zoom - missing SVG ref, transform group ref, or dimensions");
      return;
    }

    const handleTransformChange = (transform: ZoomTransform) => {
      console.log("[DEBUG] Zoom transform changed:", {
        scale: transform.k,
        translate: [transform.x, transform.y],
      });
      setZoomTransform(transform);
    };

    const zoomController = createZoomBehavior(
      svgRef.current,
      handleTransformChange,
      dimensions.width,
      dimensions.height
    );

    console.log("[DEBUG] Zoom behavior created:", zoomController);

    if (zoomController && onZoomControllerCreated) {
      console.log("[DEBUG] Notifying parent of zoom controller");
      onZoomControllerCreated(zoomController);
    }

    console.log("[DEBUG] ✅ Zoom behavior successfully applied");
  }, [dimensions.width, dimensions.height, onZoomControllerCreated]);

  const connectionColor = getConnectionColor();

  // Don't render until we have measured dimensions
  if (dimensions.width === 0 || dimensions.height === 0) {
    return (
      <div
        ref={containerRef}
        className="w-full h-full bg-graph-bg flex items-center justify-center"
      >
        <div className="text-text-secondary">Initializing graph...</div>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="w-full h-full bg-graph-bg">
      <svg
        ref={svgRef}
        width="100%"
        height="100%"
        viewBox={`0 0 ${dimensions.width} ${dimensions.height}`}
        style={{ display: "block" }}
      >
      {/* Container group for zoom/pan transformations */}
      <g
        ref={transformGroupRef}
        transform={zoomTransform?.toString() || undefined}
      >
        {/* Render connections first (behind nodes) */}
        <g className="connections">
          {mutableData.connections.map((connection, index) => {
            // D3 converts string IDs to node references after simulation start
            const source =
              typeof connection.source === "string"
                ? mutableData.nodes.find((n) => n.id === connection.source)
                : connection.source;

            const target =
              typeof connection.target === "string"
                ? mutableData.nodes.find((n) => n.id === connection.target)
                : connection.target;

            // Skip rendering if nodes aren't found or don't have positions yet
            if (
              !source ||
              !target ||
              source.x === undefined ||
              source.y === undefined ||
              target.x === undefined ||
              target.y === undefined
            ) {
              return null;
            }

            const strokeWidth = getConnectionWidth(connection.correlation);

            return (
              <line
                key={`connection-${index}`}
                x1={source.x}
                y1={source.y}
                x2={target.x}
                y2={target.y}
                stroke={connectionColor}
                strokeWidth={strokeWidth}
                strokeLinecap="round"
              />
            );
          })}
        </g>

        {/* Render nodes on top of connections */}
        <g className="nodes">
          {mutableData.nodes.map((node, nodeIndex) => {
            // Skip rendering if node doesn't have position yet
            if (node.x === undefined || node.y === undefined) {
              return null;
            }

            const fillColor = getNodeColor(node.volatility);
            const nodeRadius = getNodeRadius(node.volatility);
            const isPulsing = shouldNodePulse(node.volatility);

            // Debug logging for first 5 nodes to verify radius calculation
            if (nodeIndex < 5) {
              console.log(`[DEBUG Node ${node.id}] volatility: ${node.volatility.toFixed(3)}, radius: ${nodeRadius.toFixed(2)}px`);
            }

            return (
              <g
                key={`node-${node.id}-${nodeIndex}`}
                className={`node ${isPulsing ? 'node-high-volatility' : ''}`}
              >
                {/* Node circle */}
                <circle
                  className="node-circle"
                  cx={node.x}
                  cy={node.y}
                  r={nodeRadius}
                  fill={fillColor}
                  stroke="#1e293b"
                  strokeWidth={1.5}
                  style={{
                    cursor: "pointer",
                  }}
                />

                {/* Node label */}
                <text
                  x={node.x}
                  y={node.y - nodeRadius - 4}
                  textAnchor="middle"
                  fill="#94a3b8"
                  fontSize={10}
                  fontFamily="var(--font-geist-sans), system-ui, sans-serif"
                  style={{
                    pointerEvents: "none",
                    userSelect: "none",
                  }}
                >
                  {node.id}
                </text>
              </g>
            );
          })}
        </g>
      </g>
      </svg>
    </div>
  );
}

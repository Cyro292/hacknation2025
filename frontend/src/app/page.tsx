// ABOUTME: Main page component displaying the trading network graph
// ABOUTME: Loads graph data from API and renders the force-directed visualization

"use client";

import { useState, useMemo } from "react";
import { ForceGraph } from "@/components/ForceGraph/ForceGraph";
import { ZoomControls } from "@/components/Controls/ZoomControls";
import { SearchBar } from "@/components/Controls/SearchBar";
import { NodeInfoPanel } from "@/components/Panels/NodeInfoPanel";
import { HotTradesPanel } from "@/components/Panels/HotTradesPanel";
import { MarketPanel } from "@/components/Panels/MarketPanel";
import { useGraphData } from "@/hooks/useGraphData";
import { useMarketPanelState } from "@/hooks/useMarketPanelState";
import { isGraphData } from "@/types/graph";
import type { ZoomController } from "@/hooks/useZoom";
import type { ClusterController } from "@/types/graph";
import type { GraphNode } from "@/types/graph";

// Deduplicate nodes by ID to ensure data integrity across all components
// This prevents React key conflicts and ensures consistent behavior
function deduplicateNodes(nodes: GraphNode[]): GraphNode[] {
  const nodeMap = new Map<string, GraphNode>();
  nodes.forEach(node => {
    if (!nodeMap.has(node.id)) {
      nodeMap.set(node.id, node);
    }
  });
  return Array.from(nodeMap.values());
}

export default function Home() {
  const [zoomController, setZoomController] = useState<ZoomController | null>(null);
  const [clusterController, setClusterController] = useState<ClusterController | null>(null);
  const marketPanel = useMarketPanelState();

  // Fetch graph data from API or use mock data based on configuration
  const { data: graphData, loading, error, retry, refetch, isStale } = useGraphData({
    // Configure data fetching options
    useMockData: process.env.NEXT_PUBLIC_USE_MOCK_DATA === 'true',
    enableCache: true,
    generateTrades: true,
  });

  // Deduplicate nodes once for consistent data across all components
  // Memoized to prevent re-computation on every render
  const uniqueNodes = useMemo(() => {
    if (!graphData?.nodes) return [];
    return deduplicateNodes(graphData.nodes);
  }, [graphData]);

  // Derive the currently selected node from the cluster controller
  // Returns null if no node is selected or controller is not available
  const selectedNode = useMemo(() => {
    if (!clusterController) return null;

    const selectedNodeId = clusterController.getSelectedNodeId();
    if (!selectedNodeId) return null;

    return uniqueNodes.find(node => node.id === selectedNodeId) || null;
  }, [clusterController, uniqueNodes]);

  // Handle loading state
  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-graph-bg">
        <div className="flex flex-col items-center gap-4">
          <div className="h-16 w-16 animate-spin rounded-full border-4 border-slate-600 border-t-accent-blue"></div>
          <p className="text-lg text-slate-400">Loading top 100 markets for you...</p>
        </div>
      </div>
    );
  }

  // Handle error state
  if (error) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-graph-bg">
        <div className="flex flex-col items-center gap-4 rounded-lg bg-accent-red/10 border border-accent-red px-8 py-6">
          <p className="text-lg font-medium text-accent-red">
            {error.message || 'Failed to load graph data'}
          </p>
          <button
            onClick={retry}
            className="px-4 py-2 bg-accent-blue text-white rounded-lg hover:bg-accent-blue/80 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Validate data structure at runtime
  if (!graphData || !isGraphData(graphData)) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-graph-bg">
        <div className="flex flex-col items-center gap-4 rounded-lg bg-accent-red/10 border border-accent-red px-8 py-6">
          <p className="text-lg font-medium text-accent-red">
            Error: Invalid graph data structure
          </p>
          <button
            onClick={refetch}
            className="px-4 py-2 bg-accent-blue text-white rounded-lg hover:bg-accent-blue/80 transition-colors"
          >
            Reload
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen w-screen overflow-hidden bg-graph-bg">
      <ForceGraph
        data={graphData}
        onZoomControllerCreated={setZoomController}
        onClusterControllerCreated={setClusterController}
      />

      {/* Stale data indicator with refresh button */}
      {isStale && (
        <div className="fixed top-4 left-1/2 transform -translate-x-1/2 z-20">
          <div className="flex items-center gap-2 px-4 py-2 bg-yellow-500/10 border border-yellow-500/50 rounded-lg">
            <p className="text-sm text-yellow-500">Data is stale</p>
            <button
              onClick={refetch}
              className="px-3 py-1 text-xs bg-yellow-500 text-white rounded hover:bg-yellow-600 transition-colors"
            >
              Refresh
            </button>
          </div>
        </div>
      )}

      {clusterController && (
        <>
          <NodeInfoPanel
            nodes={uniqueNodes}
            selectedNode={selectedNode}
            onNodeSelect={(nodeId) => clusterController.selectNode(nodeId)}
            onOpenSidebar={(marketId: number) => marketPanel.openPanel(marketId)}
          />
          <SearchBar
            nodes={uniqueNodes}
            onNodeSelect={(nodeId) => clusterController.selectNode(nodeId)}
            onClear={() => clusterController.clearSelection()}
          />
          <HotTradesPanel />
        </>
      )}
      
      {/* Market Panel */}
      <MarketPanel
        marketId={marketPanel.openMarketId}
        onClose={marketPanel.closePanel}
      />
      {zoomController && (
        <ZoomControls
          onZoomIn={() => zoomController.zoomIn()}
          onZoomOut={() => zoomController.zoomOut()}
          onReset={() => zoomController.resetZoom()}
        />
      )}
    </div>
  );
}

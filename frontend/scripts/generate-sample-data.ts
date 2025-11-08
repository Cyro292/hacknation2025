// Script to generate sample-100-nodes.json using the mock data generator

import { generateGraphData, ensureValidGraphData } from "../src/utils/mockData";
import * as fs from "fs";
import * as path from "path";

// Generate the data
const graphData = generateGraphData({
  nodeCount: 100,
  avgConnectionsPerNode: 2.5,
  hotTradeCount: 15,
  seed: 42,
});

// Validate the data
ensureValidGraphData(graphData);

// Write to file
const outputPath = path.join(
  __dirname,
  "../src/data/sample-100-nodes.json"
);
fs.writeFileSync(outputPath, JSON.stringify(graphData, null, 2), "utf-8");

console.log(`✅ Generated ${graphData.nodes.length} nodes`);
console.log(`✅ Generated ${graphData.connections.length} connections`);
console.log(`✅ Generated ${graphData.hotTrades.length} hot trades`);
console.log(`✅ Sample data written to: ${outputPath}`);

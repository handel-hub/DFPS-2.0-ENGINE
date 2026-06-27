{
  pluginId: "image-processor",
  version: "1.2.0",
  extension: "__default__", // Contains the specific extension or "__default__"
  platform: "linux-bwrap",
  originalAgreements: { /* ... permissions block ... */ },
  executionPayload: {
    executable: "bwrap",
    args: [ "--unshare-all", "--ro-bind", "/data/input", "/data/input", "node", "index.js" ]
  },
  envVariables: {},
  // --- New Structural Extensions ---
  pluginMetadata: {
    pluginName: "Core Image Processor",
    pluginType: "Transform",
    description: "Applies filters and formats to image assets."
  },
  spawnPolicy: {
    runtime: "node",
    entryPoint: "index.js",
    inputDirectory: "{input}",
    outputDirectory: "{output}",
    parameters: [ { "key": "quality", "value": "85" } ]
  },
  compatibility: {
    outputs: [
      { "extension": ".png", "consumers": ["report-generator", "archive-tool"] }
    ]
  }
}
[
  {
    pluginId: "image-processor",
    pluginName: "Core Image Processor",
    pluginType: "Transform",
    description: "Applies filters and formats to image assets."
  }
]

[
  {
    pluginId: "image-processor",
    version: "1.2.0",
    executablePath: "bwrap",
    outputExtension: ".png"
  },
  {
    pluginId: "image-processor",
    version: "1.2.0(.png)",
    executablePath: "bwrap",
    outputExtension: ".png"
  }
]

[
  {
    producerPluginId: "image-processor",
    consumerPluginId: "report-generator",
    outputExtension: ".png",
    notes: "Auto-generated pipeline rule linkage via configuration manifest definitions."
  }
]
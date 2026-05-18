input

{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Job",
	"type": "object",
	"required": ["job_id", "calculatedScore", "pipeline"],
	"additionalProperties": true,
	"properties": {
		"job_id": { "type": "string", "minLength": 1 },
		"calculatedScore": { "type": "number" },
		"calculated_score": { "type": "number" },
		"pipeline": {
		"type": "object",
		"required": ["stages"],
		"properties": {
			"stages": {
			"type": "array",
			"items": {
				"type": "object",
				"required": ["stage_id", "plugin_id"],
				"properties": {
				"stage_id": { "type": "string", "minLength": 1 },
				"plugin_id": { "type": "string", "minLength": 1 },
				"depends_on": {
					"type": "array",
					"items": { "type": "string" },
					"default": []
				}
				},
				"additionalProperties": true
			},
			"minItems": 1
			}
		},
		"additionalProperties": true
		},
		"cluster_profile": { "type": "object", "additionalProperties": true }
	}
}


output 

{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "FullContextEntry",
	"type": "object",
	"required": ["job_id", "stage_id", "pluginId", "extension", "duration_ms", "memoryBytes"],
	"additionalProperties": true,
	"properties": {
		"schemaVersion": { "type": "string" },
		"job_id": { "type": "string", "minLength": 1 },
		"stage_id": { "type": "string", "minLength": 1 },
		"pluginId": { "type": "string", "minLength": 1 },
		"extension": { "type": "string", "minLength": 1 },
		"S_hat": { "type": "integer", "minimum": 0 },
		"filesize": { "type": "integer", "minimum": 0 },
		"duration_ms": { "type": "integer", "minimum": 0 },
		"memoryBytes": { "type": "integer", "minimum": 0 },
		"memMB": { "type": "number", "minimum": 0 },
		"cpu": {
		"oneOf": [
			{ "type": "number" },
			{
			"type": "object",
			"required": ["avgCpu"],
			"properties": {
				"avgCpu": { "type": "number", "minimum": 0, "maximum": 1 },
				"confidence": { "type": "number", "minimum": 0, "maximum": 1 }
			},
			"additionalProperties": true
			}
		]
		},
		"spawn_latency_ms": { "type": "integer", "minimum": 0 },
		"ioPrediction": { "type": "object", "additionalProperties": true },
		"confidence": { "type": "number", "minimum": 0, "maximum": 1 }
	}
}

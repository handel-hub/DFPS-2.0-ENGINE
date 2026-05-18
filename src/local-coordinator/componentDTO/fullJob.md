{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "RicherJob",
  "type": "object",
  "required": ["job_id", "priority_metadata", "workload_data", "data_context", "dag_recipe"],
  "additionalProperties": false,
  "properties": {
    "job_id": {
      "type": "string",
      "minLength": 1,
      "description": "Unique job identifier"
    },

    "modality": {
      "type": "string",
      "description": "Imaging modality or domain (optional)"
    },

    "priority_metadata": {
      "type": "object",
      "required": ["class", "base_priority_score", "arrival_timestamp"],
      "additionalProperties": true,
      "properties": {
        "class": { "type": "string" },
        "base_priority_score": { "type": "number" },
        "arrival_timestamp": { "type": "integer", "description": "Unix epoch seconds" }
      }
    },

    "workload_data": {
      "type": "object",
      "required": ["scaling_unit", "unit_count", "complexity_factor", "total_payload_size_mb"],
      "additionalProperties": true,
      "properties": {
        "scaling_unit": { "type": "string" },
        "unit_count": { "type": "integer", "minimum": 0 },
        "complexity_factor": { "type": "number", "minimum": 0 },
        "total_payload_size_mb": { "type": "number", "minimum": 0 },
      }
    },

	"data_context": {
		"type": "object",
		"required": ["input_uri", "output_uri", "patient_context_id"],
		"properties": {
			"input_uri": { "type": "string" },
			"output_uri": { "type": "string" },
			"patient_context_id": { "type": "string" },
			"extension": { "type": "string", "description": "File format/extension for input (e.g., dcm, nii)" },
			"mime_type": { "type": "string", "description": "Optional MIME type (e.g., application/dicom)" }
		}
	},


    "pipeline": {
      "type": "array",
      "minItems": 1,
      "stages": {
        "type": "object",
        "required": ["stage_id", "plugin_id"],
        "additionalProperties": true,
        "properties": {
          "stage_id": { "type": "string", "minLength": 1, "description": "Canonical stage identifier (was step_id)" },
          "action": { "type": "string" },
          "plugin_id": { "type": "string", "minLength": 1, "description": "Program/plugin identifier (was program_id)" },
          "depends_on": {
            "type": "array",
            "items": { "type": "string" },
            "default": []
          },
          "is_critical": { "type": "boolean", "default": false },
          "metadata": { "type": "object", "additionalProperties": true }
        }
      }
    },

    "computed": {
      "type": "object",
      "additionalProperties": true,
      "properties": {
        "calculatedScore": { "type": "number", "description": "Business priority/score for the job" }
      }
    },

    "meta": {
      "type": "object",
      "additionalProperties": true,
      "properties": {
        "schemaVersion": { "type": "string" },
        "producer": { "type": "string" }
      }
    }
  }
}

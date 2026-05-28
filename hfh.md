{
  "job_id": "CT-7701-BRAIN",
  "modality": "CT",
  
  "priority_metadata": {
    "class": "STAT",
    "base_priority_score": 100,
    "arrival_timestamp": 1713982800
  },

  "workload_data": {
    "scaling_unit": "SLICES",
    "unit_count": 550,
    "complexity_factor": 1.25,
    "total_payload_size_mb": 850
  },

  "data_context": {
    "input_uri": "s3://hospital-pacs/7701/raw/",
    "output_uri": "s3://hospital-pacs/7701/results/",
    "patient_context_id": "PAT-99-BC",
  },

  "dag_recipe": [
    {
      "step_id": "T1",
      "action": "DATA_LOAD",
      "program_id": "io_mgr_v1",
      "depends_on": [],
      "is_critical": true        
    },
    {
      "step_id": "T2",
      "action": "PRE_PROCESS",
      "program_id": "cv_core_v2",
      "depends_on": ["T1"]
    }
  ]
}

input

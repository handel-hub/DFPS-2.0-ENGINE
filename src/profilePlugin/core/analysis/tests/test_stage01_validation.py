import pytest
import math
from profilePlugin.core.analysis.stage01_validation.gateway import DataValidationGateway
from profilePlugin.core.analysis.common.errors import EmptyDatasetWarning

def test_successful_validation():
    payloads = [{
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": 150.5,
        "ProcessSpawnTime": 100.0,
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS",
        "ContextualMetadataMap": {"env": "prod"}
    }]
    
    records, report = DataValidationGateway.ingest_payload(payloads)
    
    assert len(records) == 1
    assert report.total_ingested == 1
    assert report.total_validated == 1
    assert report.rejection_count == 0
    
    record = records[0]
    assert record.plugin_id == "test-plugin"
    assert record.execution_time == 150.5
    assert record.input_size == 1024
    assert len(record.identity) == 16

def test_missing_fields_rejection():
    payloads = [{
        "PluginID": "test-plugin",
        # Missing Version
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": 150.5,
        "ProcessSpawnTime": 100.0,
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS"
    }]
    
    with pytest.raises(EmptyDatasetWarning):
        DataValidationGateway.ingest_payload(payloads)
        
    # Test capturing error without failing
    payloads.append({
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": 150.5,
        "ProcessSpawnTime": 100.0,
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS"
    })
    
    records, report = DataValidationGateway.ingest_payload(payloads)
    assert len(records) == 1
    assert report.total_ingested == 2
    assert report.rejection_count == 1
    assert "MissingFieldError" in report.error_distribution

def test_status_assertion():
    payloads = [{
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": 150.5,
        "ProcessSpawnTime": 100.0,
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "CRASH"  # Will be rejected
    }]
    
    with pytest.raises(EmptyDatasetWarning):
        DataValidationGateway.ingest_payload(payloads)

def test_temporal_paradox():
    payloads = [{
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": 50.0,
        "ProcessSpawnTime": 100.0, # Paradox: Exectime < SpawnTime
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS"
    }]
    
    with pytest.raises(EmptyDatasetWarning) as exc_info:
        DataValidationGateway.ingest_payload(payloads)

def test_physical_boundaries_negative():
    payloads = [{
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": -1024, # Negative size
        "OutputSize": 2048,
        "ExecutionTime": 150.5,
        "ProcessSpawnTime": 100.0,
        "PeakCPU": 45.5,
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS"
    }]
    
    with pytest.raises(EmptyDatasetWarning):
        DataValidationGateway.ingest_payload(payloads)

def test_nan_infinity():
    payloads = [{
        "PluginID": "test-plugin",
        "Version": "1.0.0",
        "InputSize": 1024,
        "OutputSize": 2048,
        "ExecutionTime": float('inf'), # Infinity
        "ProcessSpawnTime": 100.0,
        "PeakCPU": float('nan'), # NaN
        "AverageCPU": 20.0,
        "PeakRAM": 4096,
        "AverageRAM": 2048,
        "BytesRead": 500,
        "BytesWritten": 600,
        "ExecutionStatus": "SUCCESS"
    }]
    
    with pytest.raises(EmptyDatasetWarning):
        DataValidationGateway.ingest_payload(payloads)


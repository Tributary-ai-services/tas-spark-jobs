"""Basic schema validation tests for the events aggregator."""

import json
import sys
import os

# Add jobs directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'jobs', 'events_aggregator'))

from schema import CE_SCHEMA


def test_schema_has_required_ce_fields():
    field_names = [f.name for f in CE_SCHEMA.fields]
    for required in ['specversion', 'id', 'source', 'type']:
        assert required in field_names, f"Missing required CE field: {required}"


def test_schema_has_tas_extensions():
    field_names = [f.name for f in CE_SCHEMA.fields]
    for ext in ['tenantid', 'spaceid', 'userid', 'requestid', 'severity', 'frameworks']:
        assert ext in field_names, f"Missing TAS extension: {ext}"


def test_schema_no_snake_case_extensions():
    """CE 1.0 §4.2 forbids underscores in extension attribute names."""
    field_names = [f.name for f in CE_SCHEMA.fields]
    # These are CE core/optional fields, not extensions — underscores forbidden only in extensions
    ce_core = {'specversion', 'id', 'source', 'type', 'time', 'datacontenttype', 'subject', 'data'}
    extensions = [f for f in field_names if f not in ce_core]
    for ext in extensions:
        assert '_' not in ext, f"Extension attribute '{ext}' contains underscore — violates CE 1.0 §4.2"


def test_sample_event_matches_schema():
    """Verify a sample CloudEvent JSON would parse against the schema fields."""
    sample = {
        "specversion": "1.0",
        "id": "test-uuid",
        "source": "urn:tas:service:audimodal",
        "type": "com.tas.activity.document.uploaded",
        "time": "2026-04-16T18:00:00Z",
        "tenantid": "t-acme",
        "severity": "info",
        "data": json.dumps({"file_id": "f1"}),
    }
    field_names = {f.name for f in CE_SCHEMA.fields}
    for key in sample:
        assert key in field_names, f"Sample field '{key}' not in schema"

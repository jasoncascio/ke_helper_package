import os
import sys
from pathlib import Path
import pytest

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ke_helper import KEDatasetScanHelper

# Environment-specific variables
PROJECT_ID = "ai-learning-agents"
DATASET_NAME = "thelook"

@pytest.fixture(scope="module")
def ds_details():
    """
    Fixture to fetch dataset details once for all tests in this module.
    """
    helper = KEDatasetScanHelper(PROJECT_ID, DATASET_NAME)
    details = (
        helper
        .with_table_list_constraints(allowlist=[], blocklist=["test_optimized"])
        .with_table_ddls(True)
        .with_table_counts(True)
    ).dataset_all_details
    return details

def test_dataset_details_structure(ds_details):
    """
    Tests the basic structure and attributes of the fetched dataset details.
    """
    assert ds_details.project_id == PROJECT_ID
    assert ds_details.dataset_name == DATASET_NAME
    assert ds_details.dataset_location
    assert ds_details.dataset_description
    assert isinstance(ds_details.dataset_relationships, list)
    assert isinstance(ds_details.dataset_queries, list)
    assert isinstance(ds_details.dataset_tables, list)

def test_dataset_details_content(ds_details):
    """
    Tests the content of the fetched dataset details.
    """
    # Check that some data was returned in the lists
    assert len(ds_details.dataset_relationships) > 0
    assert len(ds_details.dataset_queries) > 0
    assert len(ds_details.dataset_tables) > 0

    # Verify table blocklist is working
    table_names = [table.name for table in ds_details.dataset_tables]
    assert "ai-learning-agents.thelook.test_optimized" not in table_names

    # Verify some expected tables are present
    assert "ai-learning-agents.thelook.users" in table_names
    assert "ai-learning-agents.thelook.orders" in table_names
    assert "ai-learning-agents.thelook.products" in table_names

    # Check that DDLs and counts were fetched
    for table in ds_details.dataset_tables:
        assert table.ddl is not None
        assert table.row_count is not None and table.row_count >= 0

def test_model_dump_json(ds_details):
    """
    Tests that the model can be dumped to a JSON string without errors.
    """
    json_string = ds_details.model_dump_json(indent=4)
    assert isinstance(json_string, str)
    assert '"project_id": "ai-learning-agents"' in json_string
    assert '"dataset_name": "thelook"' in json_string

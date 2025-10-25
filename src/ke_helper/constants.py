"""
This module stores constants for the KE Helper.
"""
DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"

# URL to list Dataplex data scans, requires project_id and location for formatting.
DATAPLEX_LIST_SCANS_URL = DATAPLEX_BASE_URL + "/projects/{project_id}/locations/{location}/dataScans"

# Resource Types
RESOURCE_TYPE_TABLE = "table"
RESOURCE_TYPE_DATASET = "dataset"

# FQN Indices
FQN_PROJECT_ID_INDEX = 4
FQN_DATASET_ID_INDEX = 6
FQN_TABLE_ID_INDEX = 8

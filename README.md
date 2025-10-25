# KE Helper: BigQuery & Dataplex Metadata Utility

`KE Helper` is a Python utility designed to simplify the process of fetching and consolidating metadata for Google BigQuery datasets. It intelligently queries the Google Cloud Dataplex API, aggregates data from various data scans (like `KNOWLEDGE_ENGINE` and `DATA_DOCUMENTATION`), and enriches it with live data from BigQuery to provide a comprehensive, structured, and easy-to-use overview of a dataset.

It answers the question: "What does this dataset contain and how do its tables relate to each other?" by providing a single, validated data object.

## Key Features

- **Unified View**: Gathers disparate metadata—dataset descriptions, business glossaries, table schemas, sample queries, and inferred relationships—into a single Pydantic model.
- **Fluent Interface**: Use chainable methods for a clean, readable, and declarative configuration.
- **Automatic Data Enrichment**: Optionally fetches table DDLs and row counts directly from BigQuery to supplement the Dataplex metadata.
- **Selective Fetching**: Easily include or exclude specific tables from the final result using allowlists and blocklists.
- **Data Validation**: Leverages Pydantic for robust, typed, and validated data models, ensuring the output is predictable and reliable.
- **Simple Authentication**: Seamlessly integrates with Google Cloud's Application Default Credentials (ADC) for secure and straightforward authentication.

## Prerequisites

### Authentication

This library uses **Application Default Credentials (ADC)**. Before running your script, ensure your environment is authenticated.

The most common way to do this is via the `gcloud` CLI:
```bash
gcloud auth application-default login
```

Your environment must have the necessary IAM permissions to read Dataplex data scans and BigQuery metadata for the target project.

### Installation

To use this utility, clone the repository and install the required dependencies.

```bash
git clone <repository-url>
cd <repository-directory>
pip install -r requirements.txt
```

## Getting Started

Using the `KEDatasetScanHelper` is a simple, three-step process: initialize, configure, and execute.

```python
from src.ke_helper import KEDatasetScanHelper

# 1. Initialize the helper with your project and dataset ID
project_id = "your-gcp-project-id"
dataset_name = "your_bigquery_dataset"

# This requires appropriate GCP authentication and permissions
helper = KEDatasetScanHelper(project_id, dataset_name)

# 2. Use the fluent interface to configure the data you need
#    Methods can be chained for a clean configuration.
dataset_details = (
    helper
    .with_table_ddls(True)  # Fetch CREATE TABLE statements
    .with_table_counts(True)  # Fetch row counts for each table
    .with_table_list_constraints(blocklist=["some_temp_table_to_ignore"]) # Ignore certain tables
).dataset_all_details # This final property access executes the fetch

# 3. Access the consolidated and validated data
print(f"Dataset Description: {dataset_details.dataset_description}")
print(f"Location: {dataset_details.dataset_location}")

# Print details for the first table found
if dataset_details.dataset_tables:
    first_table = dataset_details.dataset_tables[0]
    print(f"\n--- Details for table: {first_table.name} ---")
    print(f"Row Count: {first_table.row_count}")
    print(f"\nDDL:\n{first_table.ddl}")

    # Fields (columns) are also available
    print("\nFields:")
    for field in first_table.fields:
        print(f"  - `{field.name}`: {field.description}")


# The entire result is a Pydantic model, so you can easily
# convert it to a dictionary or JSON.
# print(dataset_details.model_dump_json(indent=2))
```

## How It Works

1.  **Initialization**: `KEDatasetScanHelper(project, dataset)` identifies the target dataset.
2.  **Scan Discovery**: It queries the Dataplex API to find all recent, successful data scans associated with that dataset and its tables.
3.  **Full Scan Fetch**: It then fetches the `FULL` view for each relevant scan to get the complete JSON payload.
4.  **Data Modeling**: The raw JSON is parsed and validated into Pydantic models (`KEScan`, `DDTableScan`, etc.) defined in `src/ke_helper/models/input_models.py`. This ensures the data is structured and typed correctly.
5.  **Enrichment (Optional)**: If configured (e.g., via `.with_table_ddls(True)`), it makes additional calls to the BigQuery API to fetch metadata not available in Dataplex, like DDL statements and row counts.
6.  **Consolidation**: All the gathered and enriched data is compiled into a single, clean output model (`KEDatasetDetails`) for the user.

This approach abstracts away the complexity of finding the right scans and stitching the data together, providing a single source of truth for your dataset's documentation.

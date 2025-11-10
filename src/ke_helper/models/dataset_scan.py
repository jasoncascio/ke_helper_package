"""
  ------------------------------------------
  Classes for consuming Knowledge Engine API 
  Specifically for view=FULL responses for Datasets
  "resource": "//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset}"
  ------------------------------------------
"""
from typing import List, Optional
from pydantic import BaseModel, Field


from .common_models import ScanBase, DDSpec, Schema, Query



class TableResult(BaseModel):
    """Contains the detailed documentation results for a specific table."""
    name: str
    overview: str
    the_schema: Schema = Field(alias="schema") # renamed to the_schema to preven collision
    queries: Optional[List[Query]] = None
    

class SchemaPath(BaseModel):
    """Represents a fully qualified column used in a join relationship."""
    table_fqn: str = Field(..., alias='tableFqn', description="Fully qualified name of the BigQuery table.")
    paths: List[str] = Field(..., alias='paths', description="The name of the column.")


class SchemaRelationship(BaseModel):
    """Defines a join relationship between two sets of columns."""
    left_schema_paths: SchemaPath = Field(..., alias='leftSchemaPaths')
    right_schema_paths: SchemaPath = Field(..., alias='rightSchemaPaths')
    sources: List[str]
    type: str = Field(..., description="The type of relationship, e.g., 'SCHEMA_JOIN'.")
    confidence_score: float = Field(..., alias='confidenceScore', description="The confidence of the relationship.")


class DDDatasetResult(BaseModel):
    overview: str
    table_results: List[TableResult] = Field(..., alias='tableResults')
    schema_relationships: Optional[List[SchemaRelationship]] = Field(None, alias='schemaRelationships')
    queries: List[Query]


class DDDataDocumentationResult(BaseModel):
    """The main result object from a DATA_DOCUMENTATION dataset scan."""
    queries: List[Query]
    dataset_result: DDDatasetResult = Field(..., alias='datasetResult')


class DDDatasetScan(ScanBase):
    """Represents a DATA_DOCUMENTATION dataset scan."""
    description: str
    display_name: str = Field(None, alias='displayName')
    data_documentation_spec: Optional[DDSpec] = Field(None, alias='dataDocumentationSpec')
    data_documentation_result: DDDataDocumentationResult = Field(..., alias='dataDocumentationResult')

    @property
    def queries(self) -> List[Query]:
        return self.data_documentation_result.queries

    @property
    def dataset_description(self) -> str:
        return self.data_documentation_result.dataset_result.overview # shortcut

    @property
    def schema_relationships(self) -> SchemaRelationship:
        return self.data_documentation_result.dataset_result.schema_relationships
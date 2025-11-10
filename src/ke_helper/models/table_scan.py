"""
  ------------------------------------------
  Classes for consuming Knowledge Engine API 
  Specifically for view=FULL responses for Tables
  "resource": "//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset}/tables/{table}"
  ------------------------------------------
"""
from typing import List, Optional
from pydantic import BaseModel, Field


from .common_models import ScanBase, DDSpec, Schema, Query, SchemaField



class DDTableResult(BaseModel):
    """The main result object from a DATA_DOCUMENTATION table scan."""
    name: Optional[str] = None
    overview: str
    the_schema: Schema = Field(alias="schema") # renamed to the_schema to preven collision
    queries: List[Query]
    

class DDTableScan(ScanBase):
    """Represents a DATA_DOCUMENTATION data scan for a table."""
    data_documentation_spec: Optional[DDSpec] = Field(None, alias='dataDocumentationSpec')
    data_documentation_result: DDTableResult = Field(..., alias='dataDocumentationResult')

    @property
    def full_table_name(self) -> str:
        parts = self.data.resource.split('/')
        return f"{parts[4]}.{parts[6]}.{parts[8]}"

    @property
    def overview(self) -> str:
        return self.data_documentation_result.overview # shortcut

    @property
    def fields(self) -> List[SchemaField]:
        return self.data_documentation_result.the_schema.fields

    @property
    def queries(self) -> List[Query]:
        return self.data_documentation_result.queries
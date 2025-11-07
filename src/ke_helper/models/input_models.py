"""
  ------------------------------------------
  Classes for consuming Knowledge Engine API
  ------------------------------------------
"""

from datetime import datetime
from typing import List, Optional, Any, Dict, ClassVar, Union
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError
from enum import Enum

from .. import constants


"""
  Generic Models for KE Scans API
"""
# class Data(BaseModel):
#     """Represents the data source for the scan."""
#     RESOURCE_TYPE_TABLE: ClassVar[str] = "table"
#     RESOURCE_TYPE_DATASET: ClassVar[str] = "dataset"

#     resource: str

#     @property
#     def is_for_table(self) -> bool:
#         return self.resource.split('/')[-2][:-1] == self.RESOURCE_TYPE_TABLE

#     @property
#     def is_for_dataset(self) -> bool:
#         return self.resource.split('/')[-2][:-1] == self.RESOURCE_TYPE_DATASET


# class OnDemand(BaseModel):
#     """Represents an on-demand trigger configuration. Empty in the provided data."""
#     pass


# class Trigger(BaseModel):
#     """Represents the trigger mechanism for a scan."""
#     on_demand: OnDemand = Field(..., alias='onDemand')


# class ExecutionSpec(BaseModel):
#     """Represents the execution specification for a scan."""
#     trigger: Trigger


# class ExecutionStatus(BaseModel):
#     """Represents the execution status of the latest job for a scan."""
#     # This field is optional as it's not present in all scan types (e.g., KNOWLEDGE_ENGINE).
#     latest_job_start_time: Optional[datetime] = Field(None, alias='latestJobStartTime')
#     latest_job_end_time: datetime = Field(..., alias='latestJobEndTime')
#     latest_job_create_time: datetime = Field(..., alias='latestJobCreateTime')


# class ScanTypeValue(Enum):
#     # KNOWLEDGE_ENGINE = "KNOWLEDGE_ENGINE" # deprecated
#     DATA_DOCUMENTATION = "DATA_DOCUMENTATION"
#     DATA_PROFILE = "DATA_PROFILE"
#     DATA_QUALITY = "DATA_QUALITY"


# class DataScan(BaseModel):
#     """Represents a single data scan item."""
#     name: str
#     uid: UUID
#     description: Optional[str] = None
#     display_name: Optional[str] = Field(None, alias='displayName') #***
#     state: str
#     create_time: datetime = Field(..., alias='createTime')
#     update_time: datetime = Field(..., alias='updateTime')
#     data: Data
#     execution_spec: ExecutionSpec = Field(..., alias='executionSpec')
#     execution_status: ExecutionStatus = Field(..., alias='executionStatus')
#     type: ScanTypeValue
#     # type: Optional[str] = ScanTypeValue.KNOWLEDGE_ENGINE.value ## TO HANDLE BUG

#     @property
#     def is_for_table(self) -> bool:
#         return self.data.is_for_table

#     @property
#     def is_for_dataset(self) -> bool:
#         return self.data.is_for_dataset

#     @property
#     def resource_name(self) -> str:
#         return self.data.resource


# class DataScansResponse(BaseModel):
#     """The root model for the entire JSON API response."""
#     data_scans: List[DataScan] = Field(..., alias='dataScans')


"""
  type KNOWLEDGE_ENGINE models
"""
# Deprecated
# class KESpec(BaseModel):
#     """Represents knowledgeEngineSpec."""
#     pass


# OLD
# class ColumnTuple(BaseModel):
#     """Represents a fully qualified column used in a join relationship."""
#     entry_fqn: str = Field(..., alias='entryFqn', description="Fully qualified name of the BigQuery table.")
#     field_path: str = Field(..., alias='fieldPath', description="The name of the column.")


## OLD
# class SchemaRelationship(BaseModel):
#     """Defines a join relationship between two sets of columns."""
#     left_columns_tuple: List[ColumnTuple] = Field(..., alias='leftColumnsTuple')
#     right_columns_tuple: List[ColumnTuple] = Field(..., alias='rightColumnsTuple')
#     type: str = Field(..., description="The type of relationship, e.g., 'JOIN'.")

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


# Deprecated
# class BusinessTerm(BaseModel):
#     """A single term and its definition from the business glossary."""
#     title: str
#     description: str

# Deprecated
# class BusinessGlossary(BaseModel):
#     """Contains a list of business terms relevant to the dataset."""
#     terms: List[BusinessTerm]

# Deprecated
# class DatasetResult(BaseModel):
#     """Contains the description, schema relationships, and glossary for a dataset."""
#     description: str
#     schema_relationship: List[SchemaRelationship] = Field(..., alias='schemaRelationship')
#     business_glossary: BusinessGlossary = Field(..., alias='businessGlossary')

# Deprecated
# class KEResult(BaseModel): 
#     """The main result object from a KNOWLEDGE_ENGINE data scan."""
#     dataset_result: DatasetResult = Field(..., alias='datasetResult')

# Deprecated
# class KEScan(DataScan):
#     """Represents a KNOWLEDGE_ENGINE data scan."""
#     knowledge_engine_spec: Optional[KESpec] = Field(None, alias='knowledgeEngineSpec')
#     knowledge_engine_result: KEResult = Field(..., alias='knowledgeEngineResult')

#     property
#     def dataset_description(self) -> str:
#         return self.knowledge_engine_result.dataset_result.description # shortcut

#     property
#     def business_glossary(self) -> BusinessGlossary:
#         return self.knowledge_engine_result.dataset_result.business_glossary

#     property
#     def schema_relationships(self) -> SchemaRelationship:
#         return self.knowledge_engine_result.dataset_result.schema_relationship

"""
  type DATA_DOCUMENTATION generic models
"""

# class DDSpec(BaseModel):
#     """Represents dataDocumentationSpec."""
#     pass


# class Query(BaseModel):
#     """Represents a single SQL query with its description."""
#     sql: str
#     description: str



"""
  type DATA_DOCUMENTATION table models
"""
# class SchemaField(BaseModel):
#     """Represents a single field (column) in a table schema."""
#     name: str
#     description: str


# class Schema(BaseModel):
#     """Represents the schema of a table, containing a list of fields."""
#     fields: List[SchemaField]


class TableResult(BaseModel):
    """Contains the detailed documentation results for a specific table."""
    # name: str # deprecated
    overview: str
    the_schema: Schema = Field(alias="schema") # renamed to the_schema to preven collision
    queries: Optional[List[Query]] = None
    # query_theme: Optional[Dict[str, Any]] = Field(None, alias='queryTheme') # deprecated


# class DDTableResult(BaseModel):
#     """The main result object from a DATA_DOCUMENTATION table scan."""
#     name: str
#     overview: str
#     the_schema: Schema = Field(alias="schema") # renamed to the_schema to preven collision
#     queries: List[Query]
#     # table_result: TableResult = Field(..., alias='tableResult')


# class DDTableScan(DataScan):
#     """Represents a DATA_DOCUMENTATION data scan."""
#     data_documentation_spec: Optional[DDSpec] = Field(None, alias='dataDocumentationSpec')
#     data_documentation_result: DDTableResult = Field(..., alias='dataDocumentationResult')

#     @property
#     def full_table_name(self) -> str:
#         parts = self.data.resource.split('/')
#         return f"{parts[4]}.{parts[6]}.{parts[8]}"

#     @property
#     def overview(self) -> str:
#         return self.data_documentation_result.table_result.overview # shortcut

#     @property
#     def fields(self) -> List[SchemaField]:
#         return self.data_documentation_result.the_schema.fields

#     @property
#     def queries(self) -> List[Query]:
#         return self.data_documentation_result.table_result.queries

"""
  type DATA_DOCUMENTATION dataset models
"""
class DDDatasetResult(BaseModel):
    overview: str
    table_results: List[TableResult] = Field(..., alias='tableResults')
    schema_relationships: Optional[List[SchemaRelationship]] = Field(None, alias='schemaRelationships')
    queries: List[Query]

class DDDataDocumentationResult(BaseModel):
    """The main result object from a DATA_DOCUMENTATION dataset scan."""
    queries: List[Query]
    dataset_result: DDDatasetResult = Field(..., alias='datasetResult')


class DDDatasetScan(DataScan):
    """Represents a DATA_DOCUMENTATION dataset scan."""
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
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
class Data(BaseModel):
    """Represents the data source for the scan."""
    resource: str

    @property
    def is_for_table(self) -> bool:
        return self.resource.split('/')[-2][:-1] == constants.RESOURCE_TYPE_TABLE

    @property
    def is_for_dataset(self) -> bool:
        return self.resource.split('/')[-2][:-1] == constants.RESOURCE_TYPE_DATASET


class OnDemand(BaseModel):
    """Represents an on-demand trigger configuration. Empty in the provided data."""
    pass


class Trigger(BaseModel):
    """Represents the trigger mechanism for a scan."""
    on_demand: OnDemand = Field(..., alias='onDemand')


class ExecutionSpec(BaseModel):
    """Represents the execution specification for a scan."""
    trigger: Trigger


class ExecutionStatus(BaseModel):
    """Represents the execution status of the latest job for a scan."""
    # This field is optional as it's not present in all scan types (e.g., KNOWLEDGE_ENGINE).
    latest_job_start_time: Optional[datetime] = Field(None, alias='latestJobStartTime')
    latest_job_end_time: Optional[datetime] = Field(..., alias='latestJobEndTime')
    latest_job_create_time: Optional[datetime] = Field(..., alias='latestJobCreateTime')


class ScanTypeValue(Enum):
    KNOWLEDGE_ENGINE = "KNOWLEDGE_ENGINE"
    DATA_DOCUMENTATION = "DATA_DOCUMENTATION"
    DATA_PROFILE = "DATA_PROFILE"
    DATA_QUALITY = "DATA_QUALITY"


class DataScan(BaseModel):
    """Represents a single data scan item."""
    name: str
    uid: UUID
    description: Optional[str] = None
    display_name: Optional[str] = Field(None, alias='displayName') #***
    state: str
    create_time: datetime = Field(..., alias='createTime')
    update_time: datetime = Field(..., alias='updateTime')
    data: Data
    execution_spec: ExecutionSpec = Field(..., alias='executionSpec')
    execution_status: ExecutionStatus = Field(..., alias='executionStatus')
    type: ScanTypeValue

    @property
    def is_for_table(self) -> bool:
        return self.data.is_for_table

    @property
    def is_for_dataset(self) -> bool:
        return self.data.is_for_dataset

    @property
    def resource_name(self) -> str:
        return self.data.resource


class DataScansResponse(BaseModel):
    """The root model for the entire JSON API response."""
    data_scans: List[DataScan] = Field(..., alias='dataScans')


"""
  type KNOWLEDGE_ENGINE models
"""
class KESpec(BaseModel):
    """Represents knowledgeEngineSpec."""
    pass


class ColumnTuple(BaseModel):
    """Represents a fully qualified column used in a join relationship."""
    entry_fqn: str = Field(..., alias='entryFqn', description="Fully qualified name of the BigQuery table.")
    field_path: str = Field(..., alias='fieldPath', description="The name of the column.")


class SchemaRelationship(BaseModel):
    """Defines a join relationship between two sets of columns."""
    left_columns_tuple: List[ColumnTuple] = Field(..., alias='leftColumnsTuple')
    right_columns_tuple: List[ColumnTuple] = Field(..., alias='rightColumnsTuple')
    type: str = Field(..., description="The type of relationship, e.g., 'JOIN'.")


class BusinessTerm(BaseModel):
    """A single term and its definition from the business glossary."""
    title: str
    description: str


class BusinessGlossary(BaseModel):
    """Contains a list of business terms relevant to the dataset."""
    terms: List[BusinessTerm]


class DatasetResult(BaseModel):
    """Contains the description, schema relationships, and glossary for a dataset."""
    description: str
    schema_relationship: List[SchemaRelationship] = Field(..., alias='schemaRelationship')
    business_glossary: BusinessGlossary = Field(..., alias='businessGlossary')


class KEResult(BaseModel):
    """The main result object from a KNOWLEDGE_ENGINE data scan."""
    dataset_result: DatasetResult = Field(..., alias='datasetResult')


class KEScan(DataScan):
    """Represents a KNOWLEDGE_ENGINE data scan."""
    knowledge_engine_spec: Optional[KESpec] = Field(None, alias='knowledgeEngineSpec')
    knowledge_engine_result: KEResult = Field(..., alias='knowledgeEngineResult')

    @property
    def dataset_description(self) -> str:
        return self.knowledge_engine_result.dataset_result.description # shortcut

    @property
    def business_glossary(self) -> BusinessGlossary:
        return self.knowledge_engine_result.dataset_result.business_glossary

    @property
    def schema_relationships(self) -> List[SchemaRelationship]:
        return self.knowledge_engine_result.dataset_result.schema_relationship


"""
  type DATA_DOCUMENTATION generic models
"""

class DDSpec(BaseModel):
    """Represents dataDocumentationSpec."""
    pass


class Query(BaseModel):
    """Represents a single SQL query with its description."""
    sql: str
    description: str



"""
  type DATA_DOCUMENTATION table models
"""
class SchemaField(BaseModel):
    """Represents a single field (column) in a table schema."""
    name: str
    description: str


class Schema(BaseModel):
    """Represents the schema of a table, containing a list of fields."""
    fields: List[SchemaField]


class TableResult(BaseModel):
    """Contains the detailed documentation results for a specific table."""
    overview: str
    the_schema: Schema = Field(alias="schema") # renamed to the_schema to preven collision
    queries: List[Query]
    query_theme: Optional[Dict[str, Any]] = Field(None, alias='queryTheme')

# Made all optional
class DDTableResult(BaseModel):
    """The main result object from a DATA_DOCUMENTATION table scan."""
    queries: Optional[List[Query]]
    overview: Optional[str]
    the_schema: Optional[Schema] = Field(alias="schema") # renamed to the_schema to preven collision
    table_result: Optional[TableResult] = Field(..., alias='tableResult')


class DDTableScan(DataScan):
    """Represents a DATA_DOCUMENTATION data scan."""
    data_documentation_spec: Optional[DDSpec] = Field(None, alias='dataDocumentationSpec')
    data_documentation_result: Optional[DDTableResult] = Field(..., alias='dataDocumentationResult')

    @property
    def full_table_name(self) -> str:
        parts = self.data.resource.split('/')
        project_id = parts[constants.FQN_PROJECT_ID_INDEX]
        dataset_id = parts[constants.FQN_DATASET_ID_INDEX]
        table_id = parts[constants.FQN_TABLE_ID_INDEX]
        return f"{project_id}.{dataset_id}.{table_id}"

    @property
    def overview(self) -> str:
        return self.data_documentation_result.table_result.overview # shortcut

    @property
    def fields(self) -> List[SchemaField]:
        return self.data_documentation_result.table_result.the_schema.fields

    @property
    def queries(self) -> List[Query]:
        return self.data_documentation_result.table_result.queries

"""
  type DATA_DOCUMENTATION dataset models
"""
class DDDatasetResult(BaseModel):
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
        return self.data_documentation_result.dataset_result.queries
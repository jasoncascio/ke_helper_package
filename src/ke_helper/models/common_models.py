"""
  ------------------------------------------
  Atomic common classes for consuming the Knowledge Engine API responses
  ------------------------------------------
"""


from datetime import datetime
from typing import Optional, ClassVar
from uuid import UUID

from pydantic import BaseModel, Field
from enum import Enum


class Query(BaseModel):
    """Represents a single SQL query with its description."""
    sql: str
    description: str


class SchemaField(BaseModel):
    """Represents a single field (column) in a table schema."""
    name: str
    description: str


class Schema(BaseModel):
    """Represents the schema of a table, containing a list of fields."""
    fields: List[SchemaField]


class DDSpec(BaseModel):
    """Represents dataDocumentationSpec."""
    pass


class ScanTypeValue(Enum):
    # KNOWLEDGE_ENGINE = "KNOWLEDGE_ENGINE" # deprecated
    DATA_DOCUMENTATION = "DATA_DOCUMENTATION"
    DATA_PROFILE = "DATA_PROFILE"
    DATA_QUALITY = "DATA_QUALITY"


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
    latest_job_end_time: datetime = Field(..., alias='latestJobEndTime')
    latest_job_create_time: datetime = Field(..., alias='latestJobCreateTime')


class Data(BaseModel):
    """Represents the data source for the scan."""
    RESOURCE_TYPE_TABLE: ClassVar[str] = "table"
    RESOURCE_TYPE_DATASET: ClassVar[str] = "dataset"

    resource: str

    @property
    def is_for_table(self) -> bool:
        return self.resource.split('/')[-2][:-1] == self.RESOURCE_TYPE_TABLE

    @property
    def is_for_dataset(self) -> bool:
        return self.resource.split('/')[-2][:-1] == self.RESOURCE_TYPE_DATASET


class ScanBase(BaseModel):
    """Represents a single data scan item."""
    name: str
    uid: UUID
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
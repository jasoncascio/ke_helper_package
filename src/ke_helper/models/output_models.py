"""
  ------------------------------------------
  Classes for output from KEDatasetScanHelper
  ------------------------------------------
"""
import json
from typing import List, Optional
from pydantic import BaseModel, Field


from .common_models import Schema, SchemaField, Query



class KEDatasetTable(BaseModel):
    """
    Represents a single table.
    """
    name: str
    overview: Optional[str] = None
    fields: List[SchemaField] = Field(..., description="A list of fields in the table.")
    queries: List[Query] = Field(..., description="A list of queries that can be run against the table.")
    ddl: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    partition_columns: Optional[List[str]] = None
    cluster_columns: Optional[List[str]] = None

    @property
    def fields_json(self) -> str:
        full_model = self.model_dump()
        return json.dumps(full_model['fields'])

    @property
    def queries_json(self) -> str:
        full_model = self.model_dump()
        return json.dumps(full_model['queries'])

    @property
    def text_field_descriptions(self) -> str:
        field_descriptions = '```\n'
        for field in self.fields:
            field_descriptions += f"`{field.name}` -- Definition: {field.description}\n"

        field_descriptions += '```'

        return field_descriptions


class KEDatasetRelationship(BaseModel):
    """
    Represents a single relationship between two database tables.
    """
    table1: str = Field(..., description="The name of the first table in the relationship.")
    table2: str = Field(..., description="The name of the second table in the relationship.")
    relationship: str = Field(..., description="The join condition that defines the relationship.")
    sources: List[str] = Field(..., description="A list of sources signals that inferred or defined this relationship.")
    confidence_score: float = Field(..., description="A confidence score for the relationship.")
    type: str = Field(..., description="The type of relationship, such as SCHEMA_JOIN")


class KEDatasetDetails(BaseModel):
    """
    Represents the detailed documentation results for a specific dataset.
    """
    project_id: str = Field(..., description="Project ID of the dataset.")
    dataset_name: str = Field(..., description="Name of the dataset")
    dataset_location: str = Field(..., description="Location of the dataset.")
    dataset_description: str = Field(..., description="A brief overview of the dataset.")
    dataset_relationships: List[KEDatasetRelationship] = Field(..., description="A list of table relationships.")
    dataset_queries: List[Query] = Field(..., description="A list of queries that can be run against the dataset.")
    # dataset_business_glossary: List[BusinessTerm] = Field(..., description="A list of business glossary terms.") # deprecated
    dataset_tables: List[KEDatasetTable] = Field(..., description="A list of tables in the dataset.")

    @property
    def dataset_relationships_json(self) -> str:
        full_model = self.model_dump()
        return json.dumps(full_model['dataset_relationships'])

    @property
    def dataset_queries_json(self) -> str:
        full_model = self.model_dump()
        return json.dumps(full_model['dataset_queries'])
    
    # deprecated
    # property
    # def dataset_glossary_terms_json(self) -> str:
    #     full_model = self.model_dump()
    #     return json.dumps(full_model['dataset_business_glossary'])

    @property
    def text_table_ddls(self) -> str:
        table_ddls = '```\n'
        for table in self.dataset_tables:
            table_ddls += f"Table: {table.name}\n"
            table_ddls += f"DDL: {table.ddl}\n"

        table_ddls += '```'
        return table_ddls
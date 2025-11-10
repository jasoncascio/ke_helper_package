"""
  ------------------------------------------
  Classes for consuming Knowledge Engine API *top level dataScans* response
  ------------------------------------------
"""
from typing import Optional
from pydantic import Field

from .common_models import ScanBase


class DataScan(ScanBase):
    """Represents a single data scan item."""
    description: Optional[str] = None
    display_name: Optional[str] = Field(None, alias='displayName')


# class DataScansResponse(BaseModel):
#     """The root model for the entire JSON API response."""
#     data_scans: List[DataScan] = Field(..., alias='dataScans')

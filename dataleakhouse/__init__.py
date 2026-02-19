"""
DataLakehouse — a Data Lakehouse implementation for Databricks.

Provides a medallion-architecture pipeline with three layers:
  - Bronze : raw data ingestion
  - Silver : cleansed and enriched data
  - Gold   : aggregated, business-ready data
"""

from .lakehouse import DataLakehouse
from .bronze import BronzeLayer
from .silver import SilverLayer
from .gold import GoldLayer

__all__ = ["DataLakehouse", "BronzeLayer", "SilverLayer", "GoldLayer"]

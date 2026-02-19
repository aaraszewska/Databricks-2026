# Databricks-2026

## Dataleakhouse

A **Data Lakehouse** library for Databricks that implements the [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) (Bronze → Silver → Gold) on top of Delta Lake.

### Architecture

```
Raw sources
    │
    ▼
┌─────────┐
│  Bronze  │  Raw data ingestion — stored as-is with lineage metadata
└────┬─────┘
     │
     ▼
┌─────────┐
│  Silver  │  Cleansed, deduplicated, and type-validated data
└────┬─────┘
     │
     ▼
┌─────────┐
│   Gold   │  Aggregated, business-ready tables served to BI / ML
└─────────┘
```

### Quick start

```python
from pyspark.sql import SparkSession
from dataleakhouse import DataLakehouse, BronzeLayer, SilverLayer, GoldLayer
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

lh = DataLakehouse(
    base_path="abfss://container@account.dfs.core.windows.net/lakehouse",
    catalog="prod",
    database="sales",
    spark=spark,
)

# --- Bronze: ingest raw JSON events ---
bronze = BronzeLayer(lh)
bronze.ingest_from_path(
    source_path="abfss://raw@account.dfs.core.windows.net/events/",
    table_name="raw_events",
    format="json",
    source_system="event_api",
)

# --- Silver: cleanse and deduplicate ---
def clean_events(df):
    return (
        df.withColumn("event_date", F.to_date("event_timestamp"))
          .withColumnRenamed("userId", "user_id")
    )

silver = SilverLayer(lh)
silver.transform(
    source_table="raw_events",
    target_table="events",
    transform_fn=clean_events,
    deduplicate_by=["event_id"],
    drop_nulls_on=["event_id", "user_id"],
)

# --- Gold: aggregate for reporting ---
gold = GoldLayer(lh)
gold.build(
    source_table="events",
    target_table="daily_event_counts",
    aggregate_fn=lambda df: df.groupBy("event_date").count(),
)
```

### Package layout

```
dataleakhouse/
├── __init__.py      # Public API
├── lakehouse.py     # DataLakehouse — shared path & Delta helpers
├── bronze.py        # BronzeLayer  — raw ingestion
├── silver.py        # SilverLayer  — cleansing & enrichment
└── gold.py          # GoldLayer    — aggregation & serving

tests/
├── test_lakehouse.py
├── test_bronze.py
├── test_silver.py
└── test_gold.py
```

### Running tests

```bash
pip install pytest
pytest tests/ -v
```

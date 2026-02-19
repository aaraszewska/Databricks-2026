"""Bronze layer — raw data ingestion.

The Bronze layer stores data in its original, unmodified form.  Every record
is stamped with ingestion metadata so that lineage can always be traced back
to the source.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .lakehouse import DataLakehouse

LAYER = "bronze"

# Metadata columns added to every Bronze record
INGEST_TIMESTAMP_COL = "_ingest_timestamp"
SOURCE_FILE_COL = "_source_file"
SOURCE_SYSTEM_COL = "_source_system"


class BronzeLayer:
    """Handles raw data ingestion into the Bronze layer.

    Parameters
    ----------
    lakehouse:
        Parent :class:`~dataleakhouse.DataLakehouse` instance.
    """

    def __init__(self, lakehouse: DataLakehouse) -> None:
        self.lakehouse = lakehouse

    # ------------------------------------------------------------------
    # Ingestion helpers
    # ------------------------------------------------------------------

    def ingest_from_path(
        self,
        source_path: str,
        table_name: str,
        format: str = "json",
        source_system: str = "unknown",
        schema: Optional[Any] = None,
        read_options: Optional[Dict[str, str]] = None,
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """Read raw files from *source_path* and write them to a Bronze Delta table.

        Parameters
        ----------
        source_path:
            Path to the source files (cloud storage path or local path).
        table_name:
            Name of the target Bronze Delta table.
        format:
            Source file format — ``"json"``, ``"csv"``, ``"parquet"``, etc.
        source_system:
            Label identifying the upstream system (stored as metadata).
        schema:
            Optional Spark ``StructType`` schema.  When *None* the schema is
            inferred automatically.
        read_options:
            Additional options forwarded to the Spark reader.
        partition_by:
            Optional list of column names to partition the Delta table by.
        """
        from pyspark.sql import functions as F  # type: ignore[import]

        reader = self.lakehouse.spark.read.format(format)
        if schema is not None:
            reader = reader.schema(schema)
        if read_options:
            for key, value in read_options.items():
                reader = reader.option(key, value)

        df = reader.load(source_path)
        df = self._add_metadata(df, source_system=source_system)

        self.lakehouse.write(
            df=df,
            layer=LAYER,
            table_name=table_name,
            mode="append",
            partition_by=partition_by,
        )

    def ingest_dataframe(
        self,
        df: Any,
        table_name: str,
        source_system: str = "unknown",
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """Write an already-loaded Spark DataFrame to a Bronze Delta table.

        Metadata columns are added before persisting.
        """
        df = self._add_metadata(df, source_system=source_system)
        self.lakehouse.write(
            df=df,
            layer=LAYER,
            table_name=table_name,
            mode="append",
            partition_by=partition_by,
        )

    # ------------------------------------------------------------------
    # Read helper
    # ------------------------------------------------------------------

    def read(self, table_name: str) -> Any:
        """Return a Spark DataFrame for the given Bronze table."""
        return self.lakehouse.read(LAYER, table_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _add_metadata(df: Any, source_system: str) -> Any:
        """Append ingestion-metadata columns to *df*."""
        from pyspark.sql import functions as F  # type: ignore[import]

        return (
            df.withColumn(INGEST_TIMESTAMP_COL, F.current_timestamp())
            .withColumn(SOURCE_FILE_COL, F.input_file_name())
            .withColumn(SOURCE_SYSTEM_COL, F.lit(source_system))
        )

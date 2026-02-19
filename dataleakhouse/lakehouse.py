"""Core DataLakehouse class providing shared utilities for all layers."""

from __future__ import annotations

from typing import Any, Dict, Optional


class DataLakehouse:
    """Represents a Data Lakehouse backed by Delta Lake tables.

    Parameters
    ----------
    base_path:
        Root storage path (e.g. ``"abfss://container@account.dfs.core.windows.net/lakehouse"``).
    catalog:
        Unity Catalog name (optional; falls back to the legacy ``hive_metastore``).
    database:
        Database / schema name inside the catalog.
    spark:
        An active ``SparkSession``.  When *None* the class can be used without
        Spark (useful for unit-testing helper logic).
    """

    def __init__(
        self,
        base_path: str,
        catalog: Optional[str] = None,
        database: str = "default",
        spark: Any = None,
    ) -> None:
        self.base_path = base_path.rstrip("/")
        self.catalog = catalog
        self.database = database
        self._spark = spark

    # ------------------------------------------------------------------
    # Spark session helpers
    # ------------------------------------------------------------------

    @property
    def spark(self) -> Any:
        """Return the attached SparkSession, raising if none is set."""
        if self._spark is None:
            raise RuntimeError(
                "No SparkSession attached. Pass `spark=<session>` when constructing "
                "DataLakehouse, or use `DataLakehouse.from_active_session()`."
            )
        return self._spark

    @classmethod
    def from_active_session(
        cls,
        base_path: str,
        catalog: Optional[str] = None,
        database: str = "default",
    ) -> "DataLakehouse":
        """Create a :class:`DataLakehouse` using the active SparkSession."""
        from pyspark.sql import SparkSession  # type: ignore[import]

        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        return cls(base_path=base_path, catalog=catalog, database=database, spark=spark)

    # ------------------------------------------------------------------
    # Path / table-name helpers
    # ------------------------------------------------------------------

    def layer_path(self, layer: str) -> str:
        """Return the storage path for *layer* (``"bronze"``, ``"silver"``, or ``"gold"``)."""
        return f"{self.base_path}/{layer}"

    def table_path(self, layer: str, table_name: str) -> str:
        """Return the full storage path for a specific Delta table."""
        return f"{self.layer_path(layer)}/{table_name}"

    def qualified_table_name(self, layer: str, table_name: str) -> str:
        """Return the fully-qualified SQL table name."""
        if self.catalog:
            return f"{self.catalog}.{self.database}_{layer}.{table_name}"
        return f"{self.database}_{layer}.{table_name}"

    # ------------------------------------------------------------------
    # Delta table operations
    # ------------------------------------------------------------------

    def read(self, layer: str, table_name: str) -> Any:
        """Read a Delta table as a Spark DataFrame."""
        path = self.table_path(layer, table_name)
        return self.spark.read.format("delta").load(path)

    def write(
        self,
        df: Any,
        layer: str,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[list] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """Write *df* to a Delta table.

        Parameters
        ----------
        df:
            Spark DataFrame to persist.
        layer:
            Target layer (``"bronze"``, ``"silver"``, or ``"gold"``).
        table_name:
            Name of the Delta table.
        mode:
            Write mode — ``"append"`` (default) or ``"overwrite"``.
        partition_by:
            Optional list of column names to partition by.
        options:
            Additional Delta writer options.
        """
        path = self.table_path(layer, table_name)
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        writer.save(path)

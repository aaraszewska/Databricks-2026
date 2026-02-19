"""Gold layer — aggregated, business-ready data.

The Gold layer materialises curated, query-optimised tables that are served
directly to BI tools, dashboards, and machine-learning feature stores.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional

from .lakehouse import DataLakehouse

LAYER = "gold"


class GoldLayer:
    """Aggregates Silver data into the Gold serving layer.

    Parameters
    ----------
    lakehouse:
        Parent :class:`~dataleakhouse.DataLakehouse` instance.
    """

    def __init__(self, lakehouse: DataLakehouse) -> None:
        self.lakehouse = lakehouse

    # ------------------------------------------------------------------
    # Aggregation helpers
    # ------------------------------------------------------------------

    def build(
        self,
        source_table: str,
        target_table: str,
        aggregate_fn: Callable[[Any], Any],
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        source_layer: str = "silver",
    ) -> None:
        """Read a Silver (or other) table, aggregate it, and write to Gold.

        Parameters
        ----------
        source_table:
            Source table name to read from.
        target_table:
            Gold table name to write to.
        aggregate_fn:
            A callable ``(df: DataFrame) -> DataFrame`` that performs the
            desired aggregation.
        mode:
            Delta write mode — ``"overwrite"`` (default) or ``"append"``.
        partition_by:
            Optional list of column names to partition the target table by.
        source_layer:
            Layer to read *source_table* from.  Defaults to ``"silver"``.
        """
        df = self.lakehouse.read(source_layer, source_table)
        df = aggregate_fn(df)

        self.lakehouse.write(
            df=df,
            layer=LAYER,
            table_name=target_table,
            mode=mode,
            partition_by=partition_by,
        )

    def build_from_sql(
        self,
        sql: str,
        target_table: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """Execute *sql* via SparkSQL and persist the result as a Gold table.

        Parameters
        ----------
        sql:
            SQL query whose result is written to *target_table*.
        target_table:
            Gold table name to write to.
        mode:
            Delta write mode — ``"overwrite"`` (default) or ``"append"``.
        partition_by:
            Optional list of column names to partition the target table by.
        """
        df = self.lakehouse.spark.sql(sql)
        self.lakehouse.write(
            df=df,
            layer=LAYER,
            table_name=target_table,
            mode=mode,
            partition_by=partition_by,
        )

    # ------------------------------------------------------------------
    # Read helper
    # ------------------------------------------------------------------

    def read(self, table_name: str) -> Any:
        """Return a Spark DataFrame for the given Gold table."""
        return self.lakehouse.read(LAYER, table_name)

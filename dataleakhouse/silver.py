"""Silver layer — cleansed and enriched data.

The Silver layer refines Bronze data by:
  - Deduplicating records
  - Casting columns to canonical types
  - Dropping rows that fail quality rules
  - Applying lightweight enrichment (e.g. parsing dates, normalising strings)
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .lakehouse import DataLakehouse

LAYER = "silver"


class SilverLayer:
    """Transforms and cleanses Bronze data into the Silver layer.

    Parameters
    ----------
    lakehouse:
        Parent :class:`~dataleakhouse.DataLakehouse` instance.
    """

    def __init__(self, lakehouse: DataLakehouse) -> None:
        self.lakehouse = lakehouse

    # ------------------------------------------------------------------
    # Transformation helpers
    # ------------------------------------------------------------------

    def transform(
        self,
        source_table: str,
        target_table: str,
        transform_fn: Callable[[Any], Any],
        deduplicate_by: Optional[List[str]] = None,
        drop_nulls_on: Optional[List[str]] = None,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """Read a Bronze table, apply transformations, and write to Silver.

        Parameters
        ----------
        source_table:
            Bronze table name to read from.
        target_table:
            Silver table name to write to.
        transform_fn:
            A callable ``(df: DataFrame) -> DataFrame`` that applies
            business-specific transformations.
        deduplicate_by:
            If provided, duplicate rows (sharing the same values for these
            columns) are dropped before writing.
        drop_nulls_on:
            If provided, rows containing *null* in any of these columns are
            removed.
        mode:
            Delta write mode — ``"append"`` (default) or ``"overwrite"``.
        partition_by:
            Optional list of column names to partition the target table by.
        """
        df = self.lakehouse.read("bronze", source_table)
        df = transform_fn(df)

        if drop_nulls_on:
            df = df.dropna(subset=drop_nulls_on)

        if deduplicate_by:
            df = df.dropDuplicates(deduplicate_by)

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
        """Return a Spark DataFrame for the given Silver table."""
        return self.lakehouse.read(LAYER, table_name)

"""Tests for the DataLakehouse core class."""

import pytest
from unittest.mock import MagicMock, patch

from dataleakhouse.lakehouse import DataLakehouse


class TestDataLakehouse:
    def test_init_defaults(self):
        lh = DataLakehouse(base_path="abfss://container@account.dfs.core.windows.net/lh")
        assert lh.base_path == "abfss://container@account.dfs.core.windows.net/lh"
        assert lh.catalog is None
        assert lh.database == "default"
        assert lh._spark is None

    def test_base_path_trailing_slash_stripped(self):
        lh = DataLakehouse(base_path="/data/lakehouse/")
        assert lh.base_path == "/data/lakehouse"

    def test_layer_path(self):
        lh = DataLakehouse(base_path="/data/lh")
        assert lh.layer_path("bronze") == "/data/lh/bronze"
        assert lh.layer_path("silver") == "/data/lh/silver"
        assert lh.layer_path("gold") == "/data/lh/gold"

    def test_table_path(self):
        lh = DataLakehouse(base_path="/data/lh")
        assert lh.table_path("bronze", "events") == "/data/lh/bronze/events"
        assert lh.table_path("gold", "daily_summary") == "/data/lh/gold/daily_summary"

    def test_qualified_table_name_with_catalog(self):
        lh = DataLakehouse(base_path="/data/lh", catalog="prod", database="sales")
        assert lh.qualified_table_name("silver", "orders") == "prod.sales_silver.orders"

    def test_qualified_table_name_without_catalog(self):
        lh = DataLakehouse(base_path="/data/lh", database="sales")
        assert lh.qualified_table_name("silver", "orders") == "sales_silver.orders"

    def test_spark_property_raises_when_not_set(self):
        lh = DataLakehouse(base_path="/data/lh")
        with pytest.raises(RuntimeError, match="No SparkSession attached"):
            _ = lh.spark

    def test_spark_property_returns_session(self):
        mock_spark = MagicMock()
        lh = DataLakehouse(base_path="/data/lh", spark=mock_spark)
        assert lh.spark is mock_spark

    def test_read_delegates_to_spark(self):
        mock_spark = MagicMock()
        lh = DataLakehouse(base_path="/data/lh", spark=mock_spark)
        lh.read("bronze", "raw_events")
        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format().load.assert_called_once_with("/data/lh/bronze/raw_events")

    def test_write_delegates_to_spark(self):
        mock_df = MagicMock()
        mock_spark = MagicMock()
        lh = DataLakehouse(base_path="/data/lh", spark=mock_spark)

        lh.write(mock_df, layer="bronze", table_name="raw_events", mode="append")

        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format().mode.assert_called_once_with("append")
        mock_df.write.format().mode().save.assert_called_once_with("/data/lh/bronze/raw_events")

    def test_write_with_partition_by(self):
        mock_df = MagicMock()
        mock_spark = MagicMock()
        lh = DataLakehouse(base_path="/data/lh", spark=mock_spark)

        lh.write(mock_df, layer="silver", table_name="orders", partition_by=["year", "month"])

        mock_df.write.format().mode().partitionBy.assert_called_once_with("year", "month")

    def test_write_with_options(self):
        mock_df = MagicMock()
        mock_spark = MagicMock()
        lh = DataLakehouse(base_path="/data/lh", spark=mock_spark)

        lh.write(
            mock_df,
            layer="gold",
            table_name="summary",
            options={"mergeSchema": "true"},
        )

        mock_df.write.format().mode().option.assert_called_once_with("mergeSchema", "true")

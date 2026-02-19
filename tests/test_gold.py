"""Tests for the Gold aggregation layer."""

import pytest
from unittest.mock import MagicMock

from dataleakhouse.lakehouse import DataLakehouse
from dataleakhouse.gold import GoldLayer


class TestGoldLayer:
    def _make_lakehouse(self):
        mock_spark = MagicMock()
        return DataLakehouse(base_path="/data/lh", spark=mock_spark)

    def test_init(self):
        lh = self._make_lakehouse()
        gold = GoldLayer(lh)
        assert gold.lakehouse is lh

    def test_build_calls_aggregate_fn(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        gold = GoldLayer(lh)
        agg_fn = MagicMock(return_value=mock_df)

        gold.build(
            source_table="events",
            target_table="daily_summary",
            aggregate_fn=agg_fn,
        )

        lh.read.assert_called_once_with("silver", "events")
        agg_fn.assert_called_once_with(mock_df)
        lh.write.assert_called_once()

    def test_build_default_mode_is_overwrite(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        gold = GoldLayer(lh)
        gold.build(
            source_table="events",
            target_table="daily_summary",
            aggregate_fn=lambda df: df,
        )

        _, kwargs = lh.write.call_args
        assert kwargs["mode"] == "overwrite"

    def test_build_custom_source_layer(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        gold = GoldLayer(lh)
        gold.build(
            source_table="bronze_events",
            target_table="daily_summary",
            aggregate_fn=lambda df: df,
            source_layer="bronze",
        )

        lh.read.assert_called_once_with("bronze", "bronze_events")

    def test_build_from_sql(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh._spark = MagicMock()
        lh._spark.sql.return_value = mock_df
        lh.write = MagicMock()

        gold = GoldLayer(lh)
        gold.build_from_sql(
            sql="SELECT date, COUNT(*) AS cnt FROM silver_default.events GROUP BY date",
            target_table="daily_counts",
        )

        lh._spark.sql.assert_called_once()
        lh.write.assert_called_once()

    def test_read_calls_lakehouse_read(self):
        lh = self._make_lakehouse()
        lh.read = MagicMock(return_value="df")
        gold = GoldLayer(lh)

        result = gold.read("daily_summary")

        lh.read.assert_called_once_with("gold", "daily_summary")
        assert result == "df"

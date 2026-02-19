"""Tests for the Silver transformation layer."""

import pytest
from unittest.mock import MagicMock

from dataleakhouse.lakehouse import DataLakehouse
from dataleakhouse.silver import SilverLayer


class TestSilverLayer:
    def _make_lakehouse(self):
        mock_spark = MagicMock()
        return DataLakehouse(base_path="/data/lh", spark=mock_spark)

    def test_init(self):
        lh = self._make_lakehouse()
        silver = SilverLayer(lh)
        assert silver.lakehouse is lh

    def test_transform_calls_transform_fn(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        silver = SilverLayer(lh)
        transform_fn = MagicMock(return_value=mock_df)

        silver.transform(
            source_table="raw_events",
            target_table="events",
            transform_fn=transform_fn,
        )

        lh.read.assert_called_once_with("bronze", "raw_events")
        transform_fn.assert_called_once_with(mock_df)
        lh.write.assert_called_once()

    def test_transform_applies_dropna(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        silver = SilverLayer(lh)
        silver.transform(
            source_table="raw_events",
            target_table="events",
            transform_fn=lambda df: df,
            drop_nulls_on=["id", "timestamp"],
        )

        mock_df.dropna.assert_called_once_with(subset=["id", "timestamp"])

    def test_transform_applies_deduplication(self):
        lh = self._make_lakehouse()
        mock_df = MagicMock()
        lh.read = MagicMock(return_value=mock_df)
        lh.write = MagicMock()

        silver = SilverLayer(lh)
        silver.transform(
            source_table="raw_events",
            target_table="events",
            transform_fn=lambda df: df,
            deduplicate_by=["id"],
        )

        mock_df.dropDuplicates.assert_called_once_with(["id"])

    def test_read_calls_lakehouse_read(self):
        lh = self._make_lakehouse()
        lh.read = MagicMock(return_value="df")
        silver = SilverLayer(lh)

        result = silver.read("events")

        lh.read.assert_called_once_with("silver", "events")
        assert result == "df"

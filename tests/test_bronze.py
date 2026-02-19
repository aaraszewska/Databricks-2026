"""Tests for the Bronze ingestion layer."""

import pytest
from unittest.mock import MagicMock, patch

from dataleakhouse.lakehouse import DataLakehouse
from dataleakhouse.bronze import BronzeLayer, INGEST_TIMESTAMP_COL, SOURCE_FILE_COL, SOURCE_SYSTEM_COL


class TestBronzeLayer:
    def _make_lakehouse(self):
        mock_spark = MagicMock()
        return DataLakehouse(base_path="/data/lh", spark=mock_spark)

    def test_init(self):
        lh = self._make_lakehouse()
        bronze = BronzeLayer(lh)
        assert bronze.lakehouse is lh

    def test_ingest_dataframe_calls_lakehouse_write(self):
        lh = self._make_lakehouse()
        lh.write = MagicMock()
        bronze = BronzeLayer(lh)

        mock_df = MagicMock()
        enriched_df = MagicMock()

        with patch.object(BronzeLayer, "_add_metadata", return_value=enriched_df):
            bronze.ingest_dataframe(mock_df, table_name="raw_events", source_system="crm")

        # Verify write was called with the bronze layer and correct table name
        lh.write.assert_called_once()
        _, kwargs = lh.write.call_args
        assert kwargs["layer"] == "bronze"
        assert kwargs["table_name"] == "raw_events"

    def test_read_calls_lakehouse_read(self):
        lh = self._make_lakehouse()
        lh.read = MagicMock(return_value="df")
        bronze = BronzeLayer(lh)

        result = bronze.read("raw_events")

        lh.read.assert_called_once_with("bronze", "raw_events")
        assert result == "df"

    def test_metadata_column_names(self):
        assert INGEST_TIMESTAMP_COL == "_ingest_timestamp"
        assert SOURCE_FILE_COL == "_source_file"
        assert SOURCE_SYSTEM_COL == "_source_system"

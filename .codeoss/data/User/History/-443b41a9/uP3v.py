import pytest
from unittest.mock import patch, MagicMock
from dags.snowflake_to_gcs_and_bigquery_with_dbt import extract_and_upload_to_gcs, verify_row_count_after_extract, create_bigquery_tables, load_all_csvs_to_bigquery

@patch('dags.snowflake_to_gcs_and_bigquery_with_dbt.SnowflakeHook')
@patch('dags.snowflake_to_gcs_and_bigquery_with_dbt.GCSHook')
def test_extract_and_upload_to_gcs(mock_gcs_hook, mock_snowflake_hook):
    mock_gcs = MagicMock()
    mock_snowflake = MagicMock()
    mock_snowflake_hook.return_value = mock_snowflake
    mock_gcs_hook.return_value = mock_gcs

    extract_and_upload_to_gcs()

    # Check if SnowflakeHook and GCSHook are called
    mock_snowflake_hook.assert_called_once()
    mock_gcs_hook.assert_called_once()
    # Check if data was uploaded
    assert mock_gcs.upload.call_count == len(extract_and_upload_to_gcs.TABLES)

@patch('dags.snowflake_to_gcs_and_bigquery_with_dbt.SnowflakeHook')
@patch('dags.snowflake_to_gcs_and_bigquery_with_dbt.GCSHook')
def test_verify_row_count_after_extract(mock_gcs_hook, mock_snowflake_hook):
    mock_gcs = MagicMock()
    mock_snowflake = MagicMock()
    mock_snowflake.get_first.return_value = [10]
    mock_gcs.download.return_value = b"column1,column2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\nvalue1,value2\n"

    verify_row_count_after_extract()

    # Verify the call count matches the number of tables
    assert mock_snowflake.get_first.call_count == len(verify_row_count_after_extract.TABLES)
    assert mock_gcs.download.call_count == len(verify_row_count_after_extract.TABLES)

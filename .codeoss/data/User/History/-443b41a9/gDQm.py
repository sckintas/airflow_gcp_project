import unittest
from unittest.mock import patch, MagicMock
from etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt import verify_row_count_after_extract

class TestVerifyRowCountAfterExtract(unittest.TestCase):
    @patch('etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt.SnowflakeHook')
    @patch('etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt.GCSHook')
    def test_verify_row_count(self, mock_gcs_hook, mock_snowflake_hook):
        mock_snowflake_instance = mock_snowflake_hook.return_value
        mock_snowflake_instance.get_first.return_value = [10]
        
        mock_gcs_instance = mock_gcs_hook.return_value
        mock_gcs_instance.download.return_value = b"header\nrow1\nrow2\nrow3\nrow4\nrow5\nrow6\nrow7\nrow8\nrow9\nrow10\n"

        verify_row_count_after_extract()

        mock_snowflake_instance.get_first.assert_called()
        mock_gcs_instance.download.assert_called()

if __name__ == '__main__':
    unittest.main()

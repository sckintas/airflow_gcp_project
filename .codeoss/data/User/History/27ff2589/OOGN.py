import unittest
from unittest.mock import patch, MagicMock
from etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt import extract_and_upload_to_gcs

class TestExtractAndUploadToGCS(unittest.TestCase):
    @patch('etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt.SnowflakeHook')
    @patch('etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt.GCSHook')
    def test_extract_and_upload(self, mock_gcs_hook, mock_snowflake_hook):
        mock_snowflake_instance = mock_snowflake_hook.return_value
        mock_snowflake_instance.get_pandas_df.return_value = MagicMock()
        
        mock_gcs_instance = mock_gcs_hook.return_value
        mock_gcs_instance.upload = MagicMock()

        extract_and_upload_to_gcs()

        mock_snowflake_instance.get_pandas_df.assert_called()
        mock_gcs_instance.upload.assert_called()

if __name__ == '__main__':
    unittest.main()

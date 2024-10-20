import unittest
from unittest.mock import patch, MagicMock
from etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt import create_bigquery_tables

class TestCreateBigQueryTables(unittest.TestCase):
    @patch('etl_airflow.dags.snowflake_to_gcs_and_bigquery_with_dbt.BigQueryCreateEmptyTableOperator')
    def test_create_tables(self, mock_bq_operator):
        mock_bq_operator_instance = mock_bq_operator.return_value
        mock_bq_operator_instance.execute = MagicMock()

        create_bigquery_tables()

        mock_bq_operator_instance.execute.assert_called()

if __name__ == '__main__':
    unittest.main()

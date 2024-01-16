import pandas as pd
import unittest
from generate_bigquery_schema_from_pandas import generate_bigquery_schema_from_pandas


class TestGenerateBigQuerySchemaFromPandas(unittest.TestCase):
    
    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = generate_bigquery_schema_from_pandas(df)
        self.assertEqual(result, [])

    def test_simple_fields(self):
        df = pd.DataFrame({
            'column1': [1, 2, 3],
            'column2': ['a', 'b', 'c'],
            'column3': [True, False, True],
        })
        result = generate_bigquery_schema_from_pandas(df)
        expected_result = [
            {'name': 'column1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'column2', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'column3', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        ]
        self.assertEqual(result, expected_result)

    def test_array_field(self):
        df = pd.DataFrame({
            'column1': [[1, 2, 3], [4, 5, 6]],
        })
        result = generate_bigquery_schema_from_pandas(df)
        expected_result = [
            {'name': 'column1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ]
        self.assertEqual(result, expected_result)

    def test_nested_field(self):
        df = pd.DataFrame({
            'column1': [{'nested1': 'value1'}, {'nested1': 'value2'}],
        })
        result = generate_bigquery_schema_from_pandas(df)
        expected_result = [
            {
                'name': 'column1',
                'type': 'RECORD',
                'mode': 'NULLABLE',
                'fields': [
                    {'name': 'nested1', 'type': 'STRING', 'mode': 'NULLABLE'}
                ]
            }
        ]
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()

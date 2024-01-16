"""Module that contains a Pandas to BigQuery schema translator."""

import datetime
import pandas as pd
import logging

from typing import Union, List, Type
from bigQuery_simple_field import BigQuerySimpleField
from bigQuery_nested_field import BigQueryNestedField
from utils import pandas_dtype_to_bigquery_type, python_type_to_pandas_dtype, convert_to_dict

logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

data = [
    {
        "column1": 10,
        "column2": 3.14,
        "column3": {"nested1": "A", "nested2": {"nested3": True}},
        "column4": ["X", "Y"],
        "column5": datetime.datetime(2023, 1, 1),
    },
    {
        "column1": 20,
        "column2": 2.71,
        "column3": {"nested1": "B", "nested2": {"nested3": False}},
        "column4": ["Z"],
        "column5": datetime.datetime(2023, 2, 1),
    },
    {
        "column1": 30,
        "column2": 1.23,
        "column3": {"nested1": "C", "nested2": {"nested3": True}},
        "column4": ["W", "V"],
        "column5": datetime.datetime(2023, 3, 1),
    },
]

df = pd.DataFrame(data)



def generate_bigquery_schema_from_pandas(df: pd.DataFrame) -> Union[List, dict, Type]:
    """
    Translate a pandas DataFrame schema into BigQuery table schema.

    Args:
        schema (dict): Pandas DataFrame schema.

    Returns:
        list: BigQuery table schema.
    """
    bigquery_schema: list[BigQueryNestedField | BigQuerySimpleField] = []

    for column, dtype in df.dtypes.items():
        field: BigQueryNestedField | BigQuerySimpleField |  None = None
        column_element = df[column][0]

        if isinstance(column_element, str):
            # Handle string type
            bigquery_type = pandas_dtype_to_bigquery_type(python_type_to_pandas_dtype(type(column_element)))
            field = BigQuerySimpleField(name=column, type=bigquery_type)

        elif isinstance(column_element, list):
            # Handle array type
            bigquery_type = pandas_dtype_to_bigquery_type(python_type_to_pandas_dtype(type(column_element[0])))
            field = BigQuerySimpleField(name=column, type=bigquery_type)

        elif isinstance(column_element, dict):
            # Handle array type
            nested_df = pd.json_normalize(column_element,max_level=0)
            bigquery_type = 'RECORD'
            field = BigQueryNestedField(column, bigquery_type, 'NULLABLE', generate_bigquery_schema_from_pandas(nested_df))

        else:
            # Handle basic types
            bigquery_type = pandas_dtype_to_bigquery_type(str(dtype))
            field = BigQuerySimpleField(name=column, type=bigquery_type)
    
        if field != None:
            bigquery_schema.append(field)
    logging.info("BigQueryFileds Schema has been generate {}".format(bigquery_schema))

    return convert_to_dict(bigquery_schema)

# Function to convert DataFrame schema to JSON
def df_to_json_schema(df: pd.DataFrame) -> dict:
    schema = {}
    for column, dtype in df.dtypes.items():
        print("col")
        print(column)
        print(dtype)
        if dtype == 'O':
            if isinstance(df[column][0], list):
                # If the column contains lists or nested structures, recursively get the schema
                #schema[column] = df_to_json_schema(df[column].to_frame())
                print("^list^^^^^")
                schema[column] = type(df[column][0][0]).__name__
            elif isinstance(df[column][0], dict):
                # If the column contains lists or nested structures, recursively get the schema
                #schema[column] = df_to_json_schema(df[column].to_frame())
                nested_df = pd.json_normalize(df[column][0],max_level=0)
                print(nested_df)
                schema[column] = df_to_json_schema(nested_df)
                print("^^^^^^^^^^^^^^^^^")
                print()
            elif isinstance(df[column][0], str):
                schema[column] = 'string'
        else:
            print("else")
            # If the column is not a list, get the data type
            schema[column] = str(df[column].dtype)
    return schema

# Example usage:
pandas_schema = {
    "Column1": "int64",
    "Column2": "float64",
    "Column3": {
        "nested1": "string",
        "nested2": {
            "nested3": "bool"
        }
    },
    "Column4": ["string"],
    "Column5": "datetime"
}

# print(df.to_json(orient="records"))

bigquery_schema = generate_bigquery_schema_from_pandas(df)
print(bigquery_schema)

print(convert_to_dict(bigquery_schema))
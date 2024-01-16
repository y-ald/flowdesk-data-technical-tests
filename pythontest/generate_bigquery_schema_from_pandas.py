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
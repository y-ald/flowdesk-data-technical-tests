import logging
from typing import Union, List, Type

logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def pandas_dtype_to_bigquery_type(pandas_type: str) -> str:
    """Converts a pandas data type to the corresponding BigQuery data type.

    Args:
        pandas_type (str): The pandas data type.

    Returns:
        str: The corresponding BigQuery data type.
    """
    # Add more type mappings as needed
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'string': 'STRING',
        'bool': 'BOOLEAN',
        'datetime': 'TIMESTAMP'
    }

    try:
        value = type_mapping.get(pandas_type)
        logging.debug(f"The value for key '{pandas_type}' is: {value}")
        return value
    
    except KeyError:
        logging.error(f"The key '{pandas_type}' is not present in the dictionary.")
    except Exception as e:
        logging.error(f"An error occurred: {e}") 


def python_type_to_pandas_dtype(python_type: Type) -> str:
    """Converts a Python type to the corresponding pandas data type.

    Args:
        python_type (Type): The Python type.

    Returns:
        str: The corresponding pandas data type.
    """
    type_to_dataframe_dtype = {
        int: 'int64',
        float: 'float64',
        str: 'string',
        bool: 'bool',
    }

    try:
        value = type_to_dataframe_dtype.get(python_type)
        logging.debug(f"The value for key '{python_type}' is: {value}")
        return value
    
    except KeyError:
        logging.error(f"The key '{python_type}' is not present in the dictionary.")
    except Exception as e:
        logging.error(f"An error occurred: {e}") 


def convert_to_dict(obj: Union[List, Type]) -> Union[List, dict, Type]:
    """Recursively converts an object to a dictionary.

    Args:
        obj (Union[List, Type]): The object to be converted.

    Returns:
        Union[List, dict, Type]: The resulting dictionary or list.
    """
    if isinstance(obj, list):
        return [convert_to_dict(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        return {k: convert_to_dict(v) for k, v in obj.__dict__.items() if v is not None}
    else:
        return obj


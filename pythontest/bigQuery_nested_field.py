from dataclasses import dataclass
from typing import Union
from bigQuery_simple_field import BigQuerySimpleField

@dataclass
class BigQueryNestedField:
    name: str
    type: str
    mode: str = 'NULLABLE'
    fields: list[Union['BigQueryNestedField', BigQuerySimpleField, None]] = None
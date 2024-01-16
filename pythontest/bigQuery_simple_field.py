from typing import Callable
from dataclasses import dataclass, asdict

@dataclass
class BigQuerySimpleField:
    name: str
    type: str
    mode: str = 'NULLABLE'
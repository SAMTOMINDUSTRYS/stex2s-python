from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class Client:
    csid: str
    name: str
    balance: float = 0
    holdings: Dict[str, int] = field(default_factory = dict)

    @property
    def stexid(self):
        return self.csid


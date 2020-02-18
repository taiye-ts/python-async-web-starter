from dataclasses import dataclass
from typing import Dict


@dataclass()
class HealthCheckStatus:
    api: bool
    db: bool

    def to_dict(self) -> Dict:
        return self.__dict__

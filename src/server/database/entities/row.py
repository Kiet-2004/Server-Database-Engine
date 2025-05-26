from typing import Dict, Any


class Row:
    def __init__(self, values: Dict[str, Any]):
        self.values = values  # e.g., {"id": 1, "name": "Alice"}

    def __getitem__(self, column_name: str):
        return self.values.get(column_name)
    
    # def to_dict(self) -> Dict[str, Any]:
    #     """
    #     Converts the Row object to a dictionary representation.
    #     """
    #     return self.values
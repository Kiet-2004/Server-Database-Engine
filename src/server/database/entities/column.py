


class Column:
    def __init__(self, name: str, dtype: str, primary_key: bool = False, nullable: bool = True):
        self.name = name
        self.dtype = dtype
        self.primary_key  = primary_key
        self.nullable = nullable

    @classmethod
    def from_dict(cls, column_dict: dict):
        """
        Create a Column instance from a dictionary.

        Args:
            column_dict (dict): A dictionary containing column attributes.

        Returns:
            Column: An instance of the Column class.
        """
        return cls(
            name=column_dict.get("name"),
            dtype=column_dict.get("type"),
            primary_key=column_dict.get("primary_key", False),
            nullable=column_dict.get("nullable", True)
        )
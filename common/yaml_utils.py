"""
To store some functions that can be useful when working with YAML files.
"""


class DictToObject:
    """
    To convert a dictionary to an object.
    """

    def __init__(self, dictionary: dict) -> None:

        for key, value in dictionary.items():
            setattr(self, key, DictToObject(value) if isinstance(value, dict) else value)

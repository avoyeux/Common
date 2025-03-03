"""
To store some functions that can be useful when working with YAML files.
"""

# IMPORTs
import os
import yaml

# IMPORTs sub
from typing import Any

# IMPORTs personal
from .main_paths import root_path

# API public
__all__ = ['ConfigToObject', 'config']



class DictToObj:
    """
    To convert a dictionary to an object.
    """

    def __init__(self, dictionary: dict[str, Any]) -> None:
        """
        Gives a class with its instance attribute names being the input dictionary keynames.

        Args:
            dictionary (dict): the dictionary to change to a class instance.
        """

        # DICTKEYs as attributes
        for key, value in dictionary.items():
            setattr(self, key, DictToObj(value) if isinstance(value, dict) else value)

    
class ConfigToObject:
    """
    To convert the config.yml file information to an instance of the class.
    """

    def __init__(self, config_path: str | None = None) -> None:
        """
        To convert the config.yml file information to an instance of the class.
        
        Args:
            config_path (str, optional): the path to the config file. When None, the config file
                is at the root_path and called 'config.yml'. Defaults to None.
        """

        # CONFIG open
        if config_path is not None:
            filepath = config_path
        else:
            filepath = os.path.join(root_path, "config.yml")
        config = self.get_config(filepath)

        # OBJECT from dict
        self._config = DictToObj(config)

    def join_constructor(
            self,
            loader: yaml.loader.SafeLoader,
            node: yaml.nodes.SequenceNode,
        ) -> str:
        """
        To add a !join constructor to join yaml path lists.

        Args:
            loader (yaml.loader.SafeLoader): the YAML loader that processes the file.
            node (yaml.nodes.SequenceNode): a YAML sequence node containing a list of strings.

        Returns:
            str: the joined path.
        """
        
        str_list = loader.construct_sequence(node)
        str_list = [value if isinstance(value, str) else "" for value in str_list]
        return os.path.join(*str_list)
    
    def  rootpath_constructor(
            self,
            loader: yaml.loader.SafeLoader,
            node: yaml.nodes.ScalarNode,
        ) -> str:
        """
        To dynamically set the root_path value in the config file.

        Args:
            loader (yaml.loader.SafeLoader): the YAML loader that processes the file.
            node (yaml.nodes.ScalarNode): a YAML scalar node containing the root_path value.

        Returns:
            str: the root_path value.
        """

        return root_path

    def get_config(self, filepath: str) -> dict[str, Any]:
        """
        To get the config file information.

        Args:
            filepath (str): the path to the config file.

        Returns:
            dict[str, Any]: the config file information as a dictionary.
        """

        # CONSTRUCTOR add
        yaml.SafeLoader.add_constructor("!join", self.join_constructor)
        yaml.SafeLoader.add_constructor("!rootpath", self.rootpath_constructor)

        with open(filepath, 'r') as conf: config = yaml.load(conf, Loader=yaml.SafeLoader)
        return config

    @property
    def config(self) -> DictToObj: return self._config

config = ConfigToObject().config

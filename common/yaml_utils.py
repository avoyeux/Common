"""
To store some functions that can be useful when working with YAML files.
"""
from __future__ import annotations

# IMPORTs
import os
import yaml

# IMPORTs personal
from .main_paths import root_path

# TYPE ANNOTATIONs
from typing import Any, Protocol

# API public
__all__ = ['ConfigToObject', 'config']



class DictToObj(Protocol):
    """
    To ignore the static attribute name checks of a class.
    """

    def __getattr__(self, name: str) -> Any: ...


class DictToObjClass:
    """
    To convert a dictionary to an object.
    """

    def __init__(self, dictionary: dict[str, Any]) -> None:
        """
        Gives a class with its instance attribute names being the input dictionary keynames.

        Args:
            dictionary (dict): the dictionary to change to a class instance.
        """

        # DICT KEYs as attributes
        for key, value in dictionary.items():
            setattr(self, key, DictToObjClass(value) if isinstance(value, dict) else value)

    def __getattr__(self, name: str) -> Any:
        """
        To be compatible with the DictToObj protocol class.

        Args:
            name (str): the name of the attribute to get.

        Returns:
            Any: check the protocol class DictToObj.
        """
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


class ConfigToObject:
    """
    To convert the config.yml file information to an instance of the class.
    """

    def __init__(self, config_path: str | None = None) -> None:
        """
        To convert the config.yml file information to an instance of the class.
        
        Args:
            config_path (str | None, optional): the path to the config file. When None, the config
                file is at the root_path and called 'config.yml'. Defaults to None.
        """

        # CONFIG open
        if config_path is not None:
            filepath = config_path
        else:
            filepath = os.path.join(root_path, "config.yml")
        config = self._get_config(filepath)

        # CREATE directories
        self._create_dirs(config)

        # OBJECT from dict
        self._config: DictToObj = DictToObjClass(config)

    @property
    def config(self) -> DictToObj:
        """
        The paths inside the config.yml file saved inside a class.
        Each instance attribute will be one of the keys inside the YAML file.

        Returns:
            DictToObj: class where the keys of the YAML file set as attribute names. 
        """
        return self._config

    def _join_constructor(
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

    def _rootpath_constructor(
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

    def _get_config(self, filepath: str) -> dict[str, Any]:
        """
        To get the config file information.

        Args:
            filepath (str): the path to the config file.

        Returns:
            dict[str, Any]: the config file information as a dictionary.
        """

        # CONSTRUCTOR add
        yaml.SafeLoader.add_constructor("!join", self._join_constructor)
        yaml.SafeLoader.add_constructor("!rootpath", self._rootpath_constructor)

        with open(filepath, 'r') as conf: config = yaml.load(conf, Loader=yaml.SafeLoader)
        return config

    def _is_a_dir(self, path: str) -> bool:
        """
        To check if the strings gotten from the YAML file is a directory path or not.

        Args:
            path (str): the string to check.

        Returns:
            bool: True if the given string is a directory path, False otherwise.
        """

        # CHECK empty
        if not isinstance(path, str) or path.strip() == "": return False

        # CHECK common paths
        if not any([os.path.sep in path, path.startswith('.'), path.startswith('~')]): return False
        if path.endswith(os.path.sep) or path.endswith('/'): return True

        expand = os.path.expanduser(path)
        normalized = os.path.normpath(expand)
        last = os.path.basename(normalized)
        if last == '': return True

        # CHECK if extension
        root, ext = os.path.splitext(last)
        if ext == '': return True

        # ELSE
        return False

    def _create_dirs(self, mapping: dict[str, Any]) -> None:
        """
        Creates directories when a value inside the YAML file represents a directory path.
        Doesn't crush the directory if it already exists.

        Args:
            mapping (dict[str, Any]): _description_
        """

        def _walk(value: Any) -> None:
            """
            To walk through the YAML file values and create the path if it is a directory.
            The directory is not created if it already exists.

            Args:
                value (Any): the current value to check.
            """
            # RECURSIVE walk
            if isinstance(value, dict):
                for v in value.values(): _walk(v)
            # CHECK string
            elif isinstance(value, str):
                if self._is_a_dir(value):
                    dirpath = os.path.expanduser(value)
                    try:
                        os.makedirs(dirpath, exist_ok=True)
                    except OSError:
                        print(
                            "\033[93mWarning: In config not create directory at path: "
                            f"{dirpath}\033[0m",
                            flush=True,
                        )
                        return
            else: return

        # RUN
        _walk(mapping)


config = ConfigToObject().config if os.path.exists(os.path.join(root_path, "config.yml")) else None

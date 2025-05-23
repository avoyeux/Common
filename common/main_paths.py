"""
Defaults functions for creating and checking the path.
"""

# IMPORTs
import os
import sys

# API public
__all__ = ["get_root_path", "root_path"]



def get_root_path() -> str:
    """
    The root path of the main directory of the project.
    If using a virtual environment, only works if the virtual environment is directly in the main
    directory. If not using a virtual environment, the Common directory needs to be directly in the
    main directory.

    Returns:
        str: the path to the main project directory.
    """

    # PATHs default
    root_env = sys.prefix
    base_python = sys.base_prefix

    # ENV virtual
    if root_env != base_python:

        # ENV at main directory
        root_directory = os.path.join(root_env, '..')
    # ENV base Python
    else:
        # ROOT 2 directories back
        current_path = os.path.dirname(os.path.abspath(__file__))
        root_directory = os.path.join(current_path, '..', '..', current_path)
    return root_directory

# CREATE rootpath
root_path = get_root_path()

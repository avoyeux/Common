#!/usr/bin/env python3.11
"""
Contains functions that I use in conjunction with the re module.
"""

# IMPORTS
import re



class RE:
    """
    Contains multipurpose functions related to the re library.
    """

    @staticmethod
    def replace_group(pattern_match: re.Match[str], groupname: str, new_value: str) -> str:
        """
        To replace only one group value of a pattern match object.

        Args:
            pattern_match (re.Match[str]): the pattern match to be changed.
            groupname (str): the group name of the pattern that needs replacing.
            new_value (str): the new string value for the given pattern.

        Returns:
            str: the new string gotten from the replacing of the specified pattern group value.
        """

        start, end = pattern_match.span(groupname)
        return pattern_match.string[:start] + new_value + pattern_match.string[end:]
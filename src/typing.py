"""
Contains the specific type annotations defined inside the 'common' package.
Setup like so for an outside user to be able to easily access the different types.
"""
from __future__ import annotations

from .yaml_utils import DictToObj
from .server_connection import ManagerAlias, SemaphoreAlias
from .png_to_video import Codec, PixFmt, Preset

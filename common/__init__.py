"""
Stores a multitude of functions that can be used in multiple repositories, hence the 'Common' name.
There are functions related to the re, matplotlib and multiprocessing libraries. Furthermore there are also utility functions for SPICE, STEREO, server connection
and general array manipulation functions.
Lastly, there is also some created useful decorators.
"""

# Imports which help for [...]
from .re import *  ## [...] the re module/library
from .hdf5 import *  ## [...] HDF5 file handling
from .plot import *  ## [...] the matplotlib library
from .dates import *  ## [...] for date reformatting
from .decorators import *  ## [...] personal decorators
from .multi_processing import *  ## [...] the multiprocessing library
from .array_manipulation import *  ## [...] manipulation of ndarray
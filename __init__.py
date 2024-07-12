"""Stores a multitude of functions that can be used in multiple repositories, hence the 'Common' name.
There are functions related to the re, matplotlib and multiprocessing libraries. Furthermore there are also utility functions for SPICE, STEREO, server connection
and general array manipulation functions.
Lastly, there is also some created useful decorators.
"""

# Imports which help for [...]
from .RE import *  ## [...] the re module/library
from .Plot import *  ## [...] the matplotlib library
from .SPICE import *  ## [...] SPICE catalogue and filename manipulation
from .STEREO import *  ## [...] STEREO image processing
from .Decorators import *  ## [...] personal decorators
from .MultiProcessing import *  ## [...] the multiprocessing library
from .ServerConnection import *  ## [...] IAS server connection
from .ArrayManipulation import *  ## [...] manipulation of ndarray
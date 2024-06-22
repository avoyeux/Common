"""To initialise the Common repository. When the development phase of the parent git repository is finished, then you can delete the .git files 
and keep only the relevant classes from the Common repository.
"""

# Imports which help for [...]
from .RE import *  ## [...] the re module/library
from .Plot import *  ## [...] the matplotlib library
from .STEREO import *  ## [...] STEREO image processing
from .common import *  ## [...] SPICE image processing
from .Decorators import *  ## [...] personal decorators
from .MultiProcessing import *  ## [...] the multiprocessing library
from .ArrayManipulation import *  ## [...] manipulation of ndarray
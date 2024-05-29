"""Has functions that help me manipulate arrays (e.g. resizing an image with a resampling method).
"""

import numpy as np
from PIL import Image
from typeguard import typechecked


class ArrayManipulation:
    """To store functions related to resampling and resizing arrays.
    """

    @typechecked
    @staticmethod
    def Downsampling(array2D: np.ndarray, downsampling_size: tuple[int, int], return_npndarray: bool = True) -> np.ndarray | Image.Image:
        """To Downsample and image using PIL with the high quality Lanczos method.
        """

        array2D = Image.fromarray(array2D)
        array2D = array2D.resize(downsampling_size, Image.Resampling.LANCZOS)
        
        if return_npndarray:
            return np.array(array2D)
        else:
            return array2D
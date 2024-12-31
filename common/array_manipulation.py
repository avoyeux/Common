#!/usr/bin/env python3.11
"""
Has functions that help me manipulate arrays (e.g. resizing an image with a resampling method).
"""

# Imports

import numpy as np
from PIL import Image



class ArrayManipulation:
    """
    To store functions related to resampling and resizing arrays.
    """

    @staticmethod
    def downsampling(array2D: np.ndarray, downsampling_size: tuple[int, ...], return_ndarray: bool = True) -> np.ndarray | Image.Image:
        """
        To Downsample and image using PIL with the high quality Lanczos method.
        
        Args:
            array2D (np.ndarray): the np.ndarray to downsample
            downsampling_size (tuple[int, ...]): the downsampling size needed.
            return_ndarray (bool, optional): deciding to return a np.ndarray or an Image.Image. Defaults to True.

        Returns:
            np.ndarray | Image.Image: the resized data.
        """

        array2D = Image.fromarray(array2D)
        array2D = array2D.resize(downsampling_size, Image.Resampling.LANCZOS)
        
        if return_ndarray: return np.array(array2D)
        return array2D
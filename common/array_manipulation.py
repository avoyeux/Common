#!/usr/bin/env python3.11
"""
Has functions that help me manipulate arrays (e.g. resizing an image with a resampling method).
"""
from __future__ import annotations

# IMPORTs alias
import numpy as np

# IMPORTs sub
from PIL import Image

# API public
__all__ = ["ArrayManipulation"]



class ArrayManipulation:
    """
    To store functions related to resampling and resizing arrays.
    """

    @staticmethod
    def downsampling(
            array2D: np.ndarray,
            downsampling_size: tuple[int, int],
            return_ndarray: bool = True,
        ) -> np.ndarray | Image.Image:
        """
        To Downsample and image using PIL with the high quality Lanczos method.
        
        Args:
            array2D (np.ndarray): the np.ndarray to downsample
            downsampling_size (tuple[int, ...]): the downsampling size needed.
            return_ndarray (bool, optional): deciding to return a np.ndarray or an Image.Image.
            Defaults to True.

        Returns:
            np.ndarray | Image.Image: the resized data.
        """

        pil_array = Image.fromarray(array2D)
        pil_array = pil_array.resize(downsampling_size, Image.Resampling.LANCZOS)
        
        if return_ndarray: return np.array(array2D)
        return array2D

#!/usr/bin/env python3.11
"""
Has functions that help me manipulate arrays (e.g. resizing an image with a resampling method).
"""
from __future__ import annotations

# IMPORTs third-party
import numpy as np
from PIL import Image

# TYPE ANNOTATIONs
from typing import TYPE_CHECKING, Any, Callable
if TYPE_CHECKING:
    import numpy.typing as npt
type AxisStatistics = Callable[[npt.NDArray], npt.NDArray]

# API public
__all__ = ["ArrayManipulation"]



class ArrayManipulation:
    """
    To store functions related to resampling and resizing arrays.
    """

    @staticmethod
    def downsampling(
            array2D: npt.NDArray,
            downsampling_size: tuple[int, int],
            return_ndarray: bool = True,
        ) -> npt.NDArray | Image.Image:
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

        if return_ndarray: return np.array(pil_array)
        return pil_array

    @staticmethod
    def binning[valueType: np.floating](
            data: npt.NDArray[valueType],
            bins: tuple[int, ...],
            statistic: AxisStatistics,
            pad: bool = True,
        ) -> npt.NDArray[valueType]:
        """
        Bins the input data given a binning tuple. The binning is done using the provided
        'statistics' argument (e.g. 'statistics = np.nanmean').
        If NaN values exist in the input data, or if 'pad' is set to True, then make sure to use a
        NaN aware callable for the 'statistics' argument. Make sure that the callable also has an
        'axis' argument.
        Furthermore, to not trim any of the input data, set 'pad' to True (data will be padded with
        NaN values accordingly). If False, then the data will be trimmed to fit the bins.

        Args:
            data (npt.NDArray[valueType]): the input data.
            bins (tuple[int, ...]): the binning tuple. The bin tuple length needs to be equal to
                the number of dimensions of 'data'.
            statistic (AxisStatistics): the statistics to use for the binning (e.g. np.nanmean,
            np.nanmedian).
            pad (bool, optional): deciding to pad the data or not. Defaults to True.

        Returns:
            npt.NDArray[valueType]: the binned data.
        """

        # CHECK dims
        if data.ndim != len(bins):
            raise ValueError(
                f"In {ArrayManipulation.__name__}, the number of bins must be the same as "
                "the number of dimensions of the input data."
            )

        # PAD data
        if pad: data = ArrayManipulationUtils.pad(data, bins)

        # TRIM
        slices = tuple(
            slice(0, (data.shape[i] // bins[i]) * bins[i])
            for i in range(data.ndim)
        )
        trimmed = data[slices]

        # RESHAPE
        pattern: list[int] = [
            item
            for dim in range(trimmed.ndim)
            for item in (trimmed.shape[dim] // bins[dim], bins[dim])
        ]
        reshaped = trimmed.reshape(pattern)

        # BIN
        axes = tuple(range(1, 2 * data.ndim, 2))
        return statistic(reshaped, axis=axes)  #type:ignore


class ArrayManipulationUtils:
    """
    Utility private functions that are used in the main 'ArrayManipulation' class.
    """

    @staticmethod
    def pad[valueType: np.floating](
            data: npt.NDArray[valueType],
            bins: tuple[int, ...],
            pad_mode: str = 'constant',
            pad_value: float = np.nan,
        ) -> npt.NDArray[valueType]:
        """
        Pads a given N-dimensional array for binning operations.
        Hence, if the data dimension size coincide with the bin values, then no padding is done.

        Returns:
            npt.NDArray[valueType]: the padded data.
        """

        # PADDING width
        pad_width = [
            (0, 0)
            if (remainder := size % bins[dim]) == 0
            else (0, bins[dim] - remainder)
            for dim, size in enumerate(data.shape)
        ]

        # PAD data
        padded = np.pad(data, pad_width, mode=pad_mode, constant_values=pad_value) #type:ignore
        return padded

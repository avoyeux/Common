"""For functions that are related to plotting data
"""

import numpy as np
from typeguard import typechecked


class PlotFunctions:
    """To store regularly used plotting functions
    """

    @typechecked
    @staticmethod
    def Contours(mask: np.ndarray) -> list[tuple[list[float], list[float]]]:
        """To plot the contours given a mask.

        Args:
            mask (np.ndarray): a boolean mask representing the mask for which the contours are needed.

        Returns:
            list[tuple[list[float], list[float]]]: list of the tuples representing the y and x coordinates of the contours.  

        Source: https://stackoverflow.com/questions/40892203/can-matplotlib-contours-match-pixel-edges
        """

        pad = np.pad(mask, [(1, 1), (1, 1)])  # zero padding
        im0 = np.abs(np.diff(pad, n=1, axis=0))[:, 1:]
        im1 = np.abs(np.diff(pad, n=1, axis=1))[1:, :]
        lines = []
        for ii, jj in np.ndindex(im0.shape):
            if im0[ii, jj] == 1:
                lines += [([ii - .5, ii - .5], [jj - .5, jj + .5])]
            if im1[ii, jj] == 1:
                lines += [([ii - .5, ii + .5], [jj - .5, jj - .5])]
        return lines
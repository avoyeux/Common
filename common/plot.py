#!/usr/bin/env python3.11
"""
For functions that are related to plotting data
"""

# IMPORTs
import scipy

# IMPORTs alias
import numpy as np

# IMPORTs sub
import scipy.interpolate
import matplotlib.pyplot as plt

# TYPE ANNOTATIONs
from typing import Any, Generator



class Plot:
    """
    To store regularly used plotting functions
    """

    @staticmethod
    def contours(mask: np.ndarray) -> list[tuple[list[float], list[float]]]:
        """
        To plot the contours given a mask.
        #TODO: need to understand why this code doesn't work if the input array is of type uint8.

        Args:
            mask (np.ndarray): a boolean mask representing the mask for which the contours are
                needed.

        Returns:
            list[tuple[list[float], list[float]]]: list of the tuples representing the y and x
                coordinates of the contours.  

        Source:
        https://stackoverflow.com/questions/40892203/can-matplotlib-contours-match-pixel-edges
        """

        pad = np.pad(mask, [(1, 1), (1, 1)])  # zero padding
        im0 = np.abs(np.diff(pad, n=1, axis=0))[:, 1:]
        im1 = np.abs(np.diff(pad, n=1, axis=1))[1:, :]
        lines = []
        for ii, jj in np.ndindex(im0.shape):
            if im0[ii, jj] == 1: lines += [([ii - .5, ii - .5], [jj - .5, jj + .5])]
            if im1[ii, jj] == 1: lines += [([ii - .5, ii + .5], [jj - .5, jj - .5])]
        return lines
    
    @staticmethod
    def random_hexadecimal_int_color_generator() -> Generator[int, None, None]:
        """
        Generator that yields a color value in integer hexadecimal code format.

        Returns:
            typing.Generator[int, None, None]: A generator that yields random integers representing
                colours in hexadecimal format.

        Yields:
            int: A random integer representing a color in hexadecimal format (in the range
                [0, 0xFFFFFF)).
        """

        while True: yield np.random.randint(0, 0xffffff)

    @staticmethod
    def different_colours(omit: list[str] = ['white']) -> Generator[str, None, None]:
        """
        To get plot colours that are really different.

        Args:
            omit (list[str], optional): the colour(s) to omit.

        Returns:
            typing.Generator[str, None, None]: the colour name
        """

        colours = [
            'white',
            'blue',
            'red',
            'brown',
            'green',
            'pink',
            'beige',
            'purple',
            'yellow',
            'gray',
            'turquoise',
            'orange',
            'black',
            'silver',
            'gold',
        ]
        colours = [c for c in colours if c not in omit]
        for c in colours: yield c


class AnnotateAlongCurve:
    """
    To annotate a curve with the values along the curve.
    """
    # todo think about if it is a plt.plot or a plt.subplot

    def __init__(
            self,
            x: np.ndarray,
            y: np.ndarray,
            arc_length: np.ndarray,
            step: int | float,
            offset: float = 0,
            annotate_kwargs: dict[str, Any] = {},
        ) -> None:

        # ARGUMENTs
        self.x = x
        self.y = y
        self.arc_length = arc_length
        self.step = step
        self.offset = offset
        self.annotate_kwargs = annotate_kwargs

        # ATTRIBUTEs
        self.gradient_dx = 5  # ? no clue in what unit it is

        # RUN
        self.x_interp, self.y_interp = self.curve_interpolation()
        self.annotate()
    
    def curve_interpolation(self) -> tuple[scipy.interpolate.interp1d, scipy.interpolate.interp1d]:
        """
        To interpolate the curve using cubic interpolation.

        Returns:
            tuple[scipy.interpolate.interp1d, scipy.interpolate.interp1d]: the x and y
                interpolation functions.
        """

        x_interp = scipy.interpolate.interp1d(self.arc_length, self.x, kind='cubic')
        y_interp = scipy.interpolate.interp1d(self.arc_length, self.y, kind='cubic')
        return x_interp, y_interp

    def annotate(self) -> None:
        """
        To annotate the curve with the values along the curve.
        """

        # POSITIONs annotation
        positions = np.arange(0, self.arc_length[-1], self.step)

        for pos in positions:

            # COORDs annotation
            x = self.x_interp(pos)
            y = self.y_interp(pos)

            # TANGENT angle
            dx = self.gradient_with_boundaries(
                interpolation=self.x_interp,
                boundaries=(self.arc_length[0], self.arc_length[-1]),
                position=float(pos),
            )
            dy = self.gradient_with_boundaries(
                interpolation=self.y_interp,
                boundaries=(self.arc_length[0], self.arc_length[-1]),
                position=float(pos),
            )
            angle = np.arctan2(dy, dx)

            # OFFSET perpendicular
            dx_offset = self.offset * np.cos(angle + np.pi / 2)
            dy_offset = self.offset * np.sin(angle + np.pi / 2)

            annotate_kwargs = {
                'fontsize': 8,
                'ha': 'center',
                'va': 'center',
                'rotation': np.rad2deg(angle),
                'color': 'grey',
                'alpha': 0.7,
                'zorder': 10,
            }
            annotate_kwargs.update(self.annotate_kwargs)

            # ANNOTATE
            plt.annotate(  # ? would this word with subplots ?
                str(round(pos)),  # ? what to do for the value precision
                xy=(x, y),
                xytext=(x + dx_offset, y + dy_offset),
                **annotate_kwargs,
            )

    def gradient_with_boundaries(
            self,
            interpolation: scipy.interpolate.interp1d,
            boundaries: tuple[float, float],
            position: float,
        ) -> float:
        """
        To compute the gradient with boundaries.
        When getting to a boundary, the gradient is computed using the first derivative.

        Args:
            dx (float): the step size used to get the gradient.
            interpolation (scipy.interpolate.interp1d): the interpolation function.
            boundaries (tuple[float, float]): the boundaries of the interpolation.
            position (int | float): the position at which to compute the gradient.

        Returns:
            int | float: the gradient at the position.
        """

        tolerance = 1e-5  # for floating points
        if (position - self.gradient_dx) - min(boundaries)  < tolerance:
            gradient = (
                (interpolation(position + self.gradient_dx) - interpolation(position))
                / self.gradient_dx
            )
        elif max(boundaries) - (position + self.gradient_dx) < tolerance:
            gradient = (
                (interpolation(position) - interpolation(position - self.gradient_dx))
                / self.gradient_dx
            )
        else:           
            gradient = (
                (
                    interpolation(position + self.gradient_dx)
                    - interpolation(position - self.gradient_dx)
                ) / (2 * self.gradient_dx)
            )
        return gradient

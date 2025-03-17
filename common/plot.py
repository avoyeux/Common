#!/usr/bin/env python3.11
"""
For functions that are related to plotting data
"""

# IMPORTS
import typing
import numpy as np



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
    def random_hexadecimal_int_color_generator() -> typing.Generator[int, None, None]:
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
    def different_colours(omit: list[str] = ['white']) -> typing.Generator[str, None, None]:
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

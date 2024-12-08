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
    

class CartesianToPolar:
    """
    To change coordinates, or an image, from the cartesian representation to polar coordinates.
    """

    def __init__(
            self,
            image: np.ndarray | None,
            coordinates: np.ndarray | None,
            center: np.ndarray | tuple[int | float, int | float],
            direction: str = 'anticlockwise',
            theta_offset: int | float = 0,
        ) -> None:

        # Attributes
        self.image = image
        self.coordinates = coordinates
        self.center = center
        self.direction = direction
        self.theta_offset = theta_offset
        self.values: np.ndarray | None = None

        # Setup
        self._initial_checks()
        
    
    def _initial_checks(self) -> None:

        # Direction keyword argument check
        direction_options = ['solar', 'clockwise', 'anticlockwise']
        if self.direction not in direction_options:
            raise ValueError(f"'{self.direction} not in permitted options. You need to choose between {', '.join(direction_options)}.")

        # Other keyword argument check
        if (self.image is None) and (self.coordinates is None):
            raise ValueError("Need to set 'image' or 'coordinates not to None.")
        elif self.coordinates is None:
            x, y = np.indices(self.image.shape)
            self.coordinates = (x.ravel(), y.ravel())
            self.values = self.image.ravel()


    def _option_setup(self):

        if self.direction == 'anticlockwise':
            polar_coordinates = self._coordinates_cartesian_to_polar() #TODO: need to add the theta offset
            #TODO: need to add the option of the values if they exist or maybe do it later if I recreate the final image

        #TODO: this might all be useless, need to check the .warp_polar documentation

    def _coordinates_cartesian_to_polar(self) -> dict[str, np.ndarray]:
        """
        To change the cartesian coordinates to the polar ones

        Returns:
            dict[str, np.ndarray]: the corresponding radial distance and polar angle as np.ndarrays.
        """

        x, y = self.coordinates
        
        # Polar coordinates
        radial_distance = np.sqrt((x - self.center[0])**2 + (y - self.center[1])**2)
        polar_angle = np.arctan2(y - self.center[1], x - self.center[0])

        polar_coordinates = {
            'radial distance': radial_distance,
            'polar angle': (polar_angle + np.pi) * 180 / np.pi,
        }
        return polar_coordinates
        

class CartesianToPolar2:
    """
    To change coordinates, or an image, from the cartesian representation to polar coordinates.
    """

    def __init__(
            self,
            image: np.ndarray | None,
            center: np.ndarray | tuple[int | float, int | float],
            output_shape: tuple[int, int],
            borders: dict[str, any],
            direction: str = 'anticlockwise',
            theta_offset: int | float = 0,
            channel_axis: None | int = None,
            **kwargs,
        ) -> None:

        # Attributes
        self.image = image
        self.center = center
        self.output_shape = output_shape
        self.borders = borders
        self.direction = direction
        self.theta_offset = theta_offset
        self.channel_axis = channel_axis

        self.kwargs = kwargs

        # Setup
        self._initial_checks()
        
    
    def _initial_checks(self) -> None:

        # Direction keyword argument check
        direction_options = ['solar', 'clockwise', 'anticlockwise']
        if self.direction not in direction_options:
            raise ValueError(f"'{self.direction} not in permitted options. You need to choose between {', '.join(direction_options)}.")
        elif self.direction == 'anticlockwise':
            polar_coordinates = self._coordinates_cartesian_to_polar()


    def _option_setup(self):

        if self.direction == 'anticlockwise':
            polar_coordinates = self._coordinates_cartesian_to_polar() #TODO: need to add the theta offset
            #TODO: need to add the option of the values if they exist or maybe do it later if I recreate the final image

        #TODO: this might all be useless, need to check the .warp_polar documentation

    def _coordinates_cartesian_to_polar(self) -> dict[str, np.ndarray]:
        """
        To change the cartesian coordinates to the polar ones

        Returns:
            dict[str, np.ndarray]: the corresponding radial distance and polar angle as np.ndarrays.
        """

        x, y = self.coordinates
        
        # Polar coordinates
        radial_distance = np.sqrt((x - self.center[0])**2 + (y - self.center[1])**2)
        polar_angle = np.arctan2(y - self.center[1], x - self.center[0])

        polar_coordinates = {
            'radial distance': radial_distance,
            'polar angle': (polar_angle + np.pi) * 180 / np.pi,
        }
        return polar_coordinates
        
        


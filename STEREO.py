"""Stores some functions that are specific for STEREO data accessing.
Right now it only reads the catalogue and reformats it to a pandas.DataFrame() object.
"""

# Imports
import os
import re

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from astropy.io import fits
from typeguard import typechecked



class StereoUtils:
    """For opening, reading and filtering the STEREO catalogue. Most functions only work when on the .ias.u-psud.fr server.

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """

    # STEREO filename pattern
    stereo_filename_pattern = re.compile(r'''(?P<date>\d{8})_
                                         (?P<ID>\d{6})_
                                         (?P<type>n\d+eu[a-zA-Z]).fts
                                         ''', re.VERBOSE)
    
    @staticmethod
    def catalogue_path() -> str:
        """Gives the full path of the STEREO catalogue. Only works when on the .ias.u-psud.fr server. Else, raises a ValueError.

        Raises:
            ValueError: when the path to the catalogue doesn't exist.

        Returns:
            str: the full path to the catalogue in Linux formatting.
        """

        path = '/archive/science_data/stereo/lz/L0/b/summary/sccB201207.img.eu'
        if not os.path.exists(path): raise ValueError('Catalogue not found. You need to be connected to the .ias.u-psud.fr server to access the archive.')
        return path
    
    @staticmethod
    def flat_path() -> str:
        """Gives the full path to the STEREO flat that should be used for the L0 files.

        Raises:
            ValueError: when the path to the flat doesn't exist.

        Returns:
            str: the full path to the flat in Linux formatting.
        """

        path = '/usr/local/ssw/stereo/secchi/calibration/20060823_wav171_fCeuB.fts'
        if not os.path.exists(path): raise ValueError('Catalogue not found. You need to be connected to the .ias.u-psud.fr server to access the archive.')
        return path
    
    @typechecked
    @staticmethod
    def read_catalogue(filter_compressed_data: bool = True, lowercase: bool = True) -> pd.DataFrame:
        """To read the STEREO catalogue and output the corresponding pandas.DataFrame object.

        Args:
            filter_compressed_data (bool, optional): Choosing to filter the highly compressed data from the catalogue. From what I was told, these files
            are so compressed that they are basically unusable. Defaults to True.
            lowercase (bool, optional): changes all the pandas.DataFrame headers to lowercase. Defaults to True.

        Returns:
            pd.DataFrame: the pandas.DataFrame object corresponding to the STEREO catalogue. The headers are (if lower_case=True):
            'filename', 'dateobs', 'tel', 'exptime', 'xsize', 'ysize', 'filter', 'polar', 'prog', 'osnum', 'dest', 'fps', 'led', 'cmprs', 'nmiss'.
        """
        
        catalogue_filepath = StereoUtils.catalogue_path()
        with open(catalogue_filepath, 'r') as catalogue:
            header_line = catalogue.readline().strip()
        headers = [header.strip() for header in header_line.split()]
        if lowercase: headers = [header.lower() for header in headers]
    
        df = pd.read_csv(catalogue_filepath, delimiter='|', skiprows=2, names=headers)
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces in the values 

        if filter_compressed_data: df = df[df['dest'] != 'SW']
        return df

    @staticmethod
    def fullpath(filenames: str | list[str] | pd.Series) -> str | list[str]:
        """Gives the fullpath to a stereo filename. 

        Args:
            filename (str | list[str]): the filename or filenames of stereo files.

        Raises:
            ValueError: when the filename doesn't correspond to the STEREO usual filename. Needed to get the file date (used in 
            the full path making).

        Returns:
            str | list[str]: the fullpath of the given STEREO filenames.
        """

        # Function initial set up
        directory_path = '/archive/science_data/stereo/lz/L0/b/img/euvi'
        if isinstance(filenames, str): filenames = [filenames]
        len_filenames = len(filenames)

        list_fullpath = [None] * len_filenames
        for i, filename in enumerate(filenames):
            pattern_match = StereoUtils.stereo_filename_pattern.match(filename)

            if pattern_match: 
                date = pattern_match.group('date')
            else:
                raise ValueError(f"STEREO filename did not match with: {filename}")
            list_fullpath[i] = os.path.join(directory_path, date, filename)
        
        if len_filenames == 1: return list_fullpath[0]
        return list_fullpath
    
    @typechecked
    @staticmethod
    def l0_l1_conversion(filenames: str | list[str] | pd.Series, ndarray: bool = True) -> np.ndarray | list[np.ndarray]:
        """To convert l0 filenames to images with the mean bias subtracted and multiplied by the flat.

        Args:
            filenames (str | list[str]): the l0 stereo filename or list of filenames. The filenames can either be the fullpath or
            just the filename with no path.
            ndarray (bool, optional): chooses if the output is a list of ndarray images or a single ndarray containing all the images.
            Defaults to True.

        Returns:
            np.ndarray | list[np.ndarray]: the ndarray images contained in a list or in a single ndarray.
        """
        
        # Making sure filenames is a list
        if isinstance(filenames, str): filenames = [filenames]

        # Getting the flat acquisition
        flat_image = fits.getdata(StereoUtils.flat_path(), 0)

        # Opening the l0 files
        len_filenames = len(filenames)
        l1_images = [None] * len_filenames
        for i, filename in enumerate(filenames):
            if not os.path.isabs(filename): filename = StereoUtils.fullpath(filename)  # checking if the filenames args is absolute path or not.

            hdul = fits.open(filename)
            image = (hdul[0].data - hdul[0].header['BIASMEAN']) #* flat_image  # TODO: this doesn't work as the flat image doesn't have the same shape
            l1_images[i] = image.astype('uint16')
            hdul.close()

        if len_filenames == 1: return l1_images[0]
        if ndarray: return np.stack(l1_images, axis = 0)
        return l1_images

    @typechecked
    @staticmethod
    def image_preprocessing(images: np.ndarray | list[np.ndarray], clip_percentages: tuple[int | float, int | float] = (1, 99.99), clip_nan: bool = True,
                            log: bool = True) -> np.ndarray:
        
        if isinstance(images, str): images = np.stack(images, axis=0)

        # Getting the extrema
        lower_cut = np.nanpercentile(images, clip_percentages[0])
        higher_cut = np.nanpercentile(images, clip_percentages[1])

        # Clipping
        images[images < lower_cut] = lower_cut
        images[images > higher_cut] = higher_cut

        # Swapping nan values
        if clip_nan: images = np.where(np.isnan(images), lower_cut, images)
        if log: images = np.log(images)
        return images


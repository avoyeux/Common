"""Stores some functions that are specific for STEREO data accessing.
Right now it only reads the catalogue and reformats it to a pandas.DataFrame() object.
"""

# Imports
import os
import re

import numpy as np
import pandas as pd

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
        return df.reset_index(drop=True)

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
    def image_preprocessing(filenames: str | list[str] | pd.Series, clip_percentages: tuple[int | float, int | float] = (1, 99.99), clip_nan: bool = True,
                            log: bool = True, log_lowercut: int = 1) -> np.ndarray:
        
        if isinstance(filenames, str): 
            filenames = [filenames]

        len_filenames = len(filenames)
        images = [None] * len_filenames
        means = [None] * len_filenames
        for i, filename in enumerate(filenames):
            if not os.path.isabs(filename): filename = StereoUtils.fullpath(filename)

            hdul = fits.open(filename)
            images[i] = hdul[0].data.astype('uint16')
            means[i] = hdul[0].header['BIASMEAN']
            hdul.close()
        images = np.stack(images, axis=0)
        means = np.stack(means, axis=0)
        means = means.reshape(len_filenames, 1, 1)

        # Getting the extrema
        lower_cut = np.nanpercentile(images, clip_percentages[0])
        higher_cut = np.nanpercentile(images, clip_percentages[1])

        # Clipping
        images[images < lower_cut] = lower_cut
        images[images > higher_cut] = higher_cut

        # Swapping nan values
        if clip_nan: images = np.where(np.isnan(images), lower_cut, images)
        images = images - means  # subtracting the bias
        if log: 
            images[images < log_lowercut] = log_lowercut
            images = np.log(images)

        if images.shape[0] == 1: return images[0]
        return images


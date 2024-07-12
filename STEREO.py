#!/usr/bin/env python3.11
"""
Stores some functions that are specific for STEREO data accessing.
"""

# Imports
import os
import re

import numpy as np
import pandas as pd

from astropy.io import fits

# Personal libraries
from .ServerConnection import ServerUtils


class StereoUtils:
    """For opening, reading and filtering the STEREO catalogue. Most functions only work when on the .ias.u-psud.fr server.

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """

    # STEREO filename pattern and main paths.
    stereo_filename_pattern = re.compile(r'''(?P<date>\d{8})_
                                         (?P<ID>\d{6})_
                                         (?P<type>n\d+eu[a-zA-Z]).fts
                                         ''', re.VERBOSE)
    file_directory_path = '/archive/science_data/stereo/lz/L0/b/img/euvi'  # main directory where the STEREO FITS files are
    flat_path = '/usr/local/ssw/stereo/secchi/calibration/20060823_wav171_fCeuB.fts'  # path to the flat field image albeit useless as just ones.
    catalogue_path = '/archive/science_data/stereo/lz/L0/b/summary/sccB201207.img.eu'  # path to the STEREO catalogue
    
    @staticmethod
    def read_catalogue(filter_compressed_data: bool = True, lowercase: bool = True, verbose: int = 0) -> pd.DataFrame:
        """
        To read the STEREO catalogue and output the corresponding pandas.DataFrame object.

        Args:
            filter_compressed_data (bool, optional): Choosing to filter the highly compressed data from the catalogue. From what I was told, these files
            are so compressed that they are basically unusable. Defaults to True.
            lowercase (bool, optional): changes all the pandas.DataFrame headers to lowercase. Defaults to True.
            verbose (int, optional): defines the level of the prints. 0 means none and the higher the more low level are the prints. Defaults to 0.

        Returns:
            pd.DataFrame: the pandas.DataFrame object corresponding to the STEREO catalogue. The headers are (if lower_case=True):
            'filename', 'dateobs', 'tel', 'exptime', 'xsize', 'ysize', 'filter', 'polar', 'prog', 'osnum', 'dest', 'fps', 'led', 'cmprs', 'nmiss'.
        """
        
        # Server connection check
        # if not os.path.exists(StereoUtils.catalogue_path): catalogue_buffer = ServerUtils.ssh_connect(StereoUtils.catalogue_path, verbose=verbose)
        # TODO: can only add this part when I change the connection to a temporary drive creation

        with open(StereoUtils.catalogue_path, 'r') as catalogue: header_line = catalogue.readline().strip()
        headers = [header.strip() for header in header_line.split()]
        if lowercase: headers = [header.lower() for header in headers]
    
        df = pd.read_csv(StereoUtils.catalogue_path, delimiter='|', skiprows=2, names=headers)
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces in the values 

        if filter_compressed_data: df = df[df['dest'] != 'SW']  # not using the compressed data
        return df.reset_index(drop=True)

    @staticmethod
    def fullpath(filenames: str | list[str] | pd.Series) -> str | list[str]:
        """
        Gives the fullpath to a stereo filename. 

        Args:
            filename (str | list[str]): the filename or filenames of stereo files.

        Raises:
            ValueError: when the filename doesn't correspond to the STEREO usual filename. Needed to get the file date (used in 
            the full path making).

        Returns:
            str | list[str]: the fullpath of the given STEREO filenames.
        """
        
        # Type changing
        if isinstance(filenames, str): filenames = [filenames]
        
        len_filenames = len(filenames)
        list_fullpath = [None] * len_filenames
        for i, filename in enumerate(filenames):
            pattern_match = StereoUtils.stereo_filename_pattern.match(filename)

            if pattern_match: 
                date = pattern_match.group('date')
                list_fullpath[i] = os.path.join(StereoUtils.file_directory_path, date, filename)
            else:
                raise ValueError(f"STEREO filename did not match with: {filename}")    
        
        if len_filenames == 1: return list_fullpath[0]
        return list_fullpath

    @staticmethod
    def image_preprocessing(filenames: str | list[str] | pd.Series, clip_percentages: tuple[int | float, int | float] = (1, 99.99), clip_nan: bool = True,
                            log: bool = True, log_lowercut: int = 1) -> np.ndarray:
        """Not finished yet as I don't remember exactly what needs to be kept and what shouldn't"""
        
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


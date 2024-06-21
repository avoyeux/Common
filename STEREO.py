"""Stores some functions that are specific for STEREO data accessing.
Right now it only reads the catalogue and reformats it to a pandas.DataFrame() object.
"""

# Imports
import os
import re
import pandas as pd

from typeguard import typechecked


class StereoUtils:
    """For opening, reading and filtering the STEREO catalogue.

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
    
    @typechecked
    @staticmethod
    def read_catalogue(filter_compressed_data: bool = True) -> pd.DataFrame:
        """To read the STEREO catalogue and output the corresponding pandas.DataFrame object.

        Args:
            filter_compressed_data (bool, optional): Choosing to filter the highly compressed data from the catalogue. From what I was told, these files
            are so compressed that they are basically unusable. Defaults to True.

        Returns:
            pd.DataFrame: the pandas.DataFrame object corresponding to the STEREO catalogue. The headers are:
            'FileName', 'DateObs', 'Tel', 'Exptime', 'Xsize', 'Ysize', 'Filter', 'Polar', 'Prog', 'OSnum', 'Dest', 'FPS', 'LED', 'CMPRS', 'NMISS'.
        """
        df = pd.read_csv(StereoUtils.catalogue_path(), delimiter='|', comment='=', engine='python')

        # Remove trailing whitespaces in the column names and values
        df.columns = [col.strip() for col in df.columns]
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'string' else x)

        if filter_compressed_data: df = df[df['Dest'] != 'SW']
        return df

    @staticmethod
    def full_path(filename: str) -> str:
        directory_path = '/archive/science_data/stereo/lz/L0/b/img/euvi'
        pattern_match = StereoUtils.stereo_filename_pattern.match(filename)

        if pattern_match: 
            date = pattern_match.group('date')
        else:
            raise ValueError(f"STEREO filename did not match with: {filename}")
        return os.path.join(directory_path, date, filename)
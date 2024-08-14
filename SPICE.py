#!/usr/bin/env python3.11
"""
Stores useful functions related to the SPICE data.
It mostly copy of the common.py file of Dr. Gabriel Pelouze for whom I had worked for.
Only small changes were applied to better correspond to my usage and code style (I don't really follow the PEP 8 guidelines...).
Furthermore. some functions that I don't need were taken out.
This code is actually one of the first 'proper' codes that I have used and the basics for the Common repository I have created.

Dr. Pelouze's github can be found here: https://github.com/gpelouze
"""

# IMPORTS
import re
import os
import pandas as pd

from dateutil.parser import parse as parse_date

# Personal libraries
from .ServerConnection import SSHMirroredFilesystem


class SpiceUtils:
    """
    Stores some useful functions to access and manipulate the SPICE catalogue and corresponding filenames.
    """

    re_spice_L123_filename = re.compile(
        r"""
        solo
        _(?P<level>L[123])
        _spice
            (?P<concat>-concat)?
            -(?P<slit>[wn])
            -(?P<type>(ras|sit|exp))
            (?P<db>-db)?
            (?P<int>-int)?
        _(?P<time>\d{8}T\d{6})
        _(?P<version>V\d{2})
        _(?P<SPIOBSID>\d+)-(?P<RASTERNO>\d+)
        \.fits
        """,
        re.VERBOSE)

    @staticmethod
    def read_spice_uio_catalog(verbose: int = 0, flush: bool = False) -> pd.DataFrame:
        """
        Read csv table SPICE FITS files catalog. Works on the server or locally if the ~/.ssh/config is properly set-up using a key and 'sol' as the ssh connection
        shortcut (c.f. Common.ServerConnection.ServerUtils.ssh_connect()).

        Args:
            verbose (int, optional): defines the level of the prints. 0 means none and the higher the more low level are the prints. Defaults to 0.
            flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to force the prints or not. 
                Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted exactly when it is called 
                (usually not the case when multiprocessing). Defaults to False.

        Returns:
            pd.DataFrame: the SPICE catalogue.


        Example queries that can be done on the result:

        * `df[(df.LEVEL == "L2") & (df["DATE-BEG"] >= "2020-11-17") \
          & (df["DATE-BEG"] < "2020-11-18") & (df.XPOSURE > 60.)]`
        * `df[(df.LEVEL == "L2") \
          & (df.STUDYDES == "Standard dark for cruise phase")]`

        Source: https://spice-wiki.ias.u-psud.fr/doku.php/data:data_analysis_manual:read_catalog_python
        """

        # Setup
        main_path = os.path.join('/archive', 'SOLAR-ORBITER', 'SPICE')
        catalogue_filepath = os.path.join(main_path, 'fits', 'spice_catalog.csv')
        date_columns = ['DATE-BEG', 'DATE', 'TIMAQUTC']

        # Finding the file
        if os.path.exists(main_path):
            df = pd.read_csv(catalogue_filepath, low_memory=False, na_values="MISSING", parse_dates=date_columns)
        else:
            if verbose > 1: print(f"\033[37mCouldn't find the SPICE archive. Connecting to the server ...\033[0m", flush=flush)

            # Get file from the server
            catalogue_filepath = SSHMirroredFilesystem.remote_to_local(catalogue_filepath)
            df = pd.read_csv(catalogue_filepath, low_memory=False, na_values="MISSING", parse_dates=date_columns)

            # Cleanup temporary folder
            SSHMirroredFilesystem.cleanup(which='sameIDLatest')

        # Striping the useless spaces
        df.LEVEL = df.LEVEL.apply(lambda string: string.strip() if isinstance(string, str) else string)
        df.STUDYTYP = df.STUDYTYP.apply(lambda string: string.strip() if isinstance(string, str) else string)
        return df

    @staticmethod
    def parse_filename(filename: str) -> dict[str, str]:
        """
        Parsing the filename using a re.Pattern.

        Args:
            filename (str): the filename of a SPICE FITS file.

        Raises:
            ValueError: raises a ValueError if the re.Match object wasn't successful.

        Returns:
            dict[str, str]: the result of the re.Match as a dict[str, str].
        """

        m = SpiceUtils.re_spice_L123_filename.match(filename)
        if m is None: raise ValueError(f'Could not parse SPICE filename: {filename}')
        return m.groupdict()

    @staticmethod
    def ias_fullpath(filenames: str | list[str]) -> str | list[str]:
        """
        Gives the server fullpath to a SPICE FITS file given it's filename(s).

        Args:
            filenames (str | list[str]): SPICE FITS filename(s).

        Returns:
            str | list[str]: the server (or local if the idc-archive remote drive is set up) fullpath to the corresponding SPICE FITS file(s).
        """

        # Initial type conversion
        if isinstance(filenames, str): filenames = [filenames]

        # Check if connected to the server through a remote drive
        drive_path = os.path.join('//idc-archive', 'SOLO', 'SPICE')
        main_path = drive_path if os.path.exists(drive_path) else os.path.join('/archive', 'SOLAR-ORBITER', 'SPICE')

        fullpaths = [None] * len(filenames)
        for i, filename in enumerate(filenames):
            d = SpiceUtils.parse_filename(filename)
            date = parse_date(d['time'])
            fullpaths[i] = os.path.join(main_path, 'fits', 'level' + d['level'].lstrip('L'), f'{date.year:04d}', f'{date.month:02d}', f'{date.day:02d}', filename)

        if len(fullpaths) == 1: return fullpaths[0]
        return fullpaths


def get_mosaic_filenames(verbose: int = 0, flush: bool = False) -> list[str]:
    """
    To filter the SPICE catalogue to only get the mosaic event related filenames

    Args:
        verbose (int, optional): defines the level of the prints. 0 means none and the higher the more low level are the prints. Defaults to 0.
        flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to force the prints or not. 
            Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted exactly when it is called 
            (usually not the case when multiprocessing). Defaults to False.

    Returns:
        list[str]: the list of the SPICE FITS filenames corresponding to the mosaic event.
    """

    cat = SpiceUtils.read_spice_uio_catalog(verbose=verbose, flush=flush)
    filters = (
        (cat['LEVEL'] == 'L2')
        & (cat['MISOSTUD'] == '2093')
        & (cat['DATE-BEG'] >= '2022-03-07T06:59:59')
        & (cat['DATE-BEG'] <= '2023-03-07T11:29:59')
        )
    res = cat[filters]
    return list(res['FILENAME'])

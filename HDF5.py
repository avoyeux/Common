#!/usr/bin/env python3.11
"""
To help when using the HDF5 format. Initial created to have a similar option than the .info() in FITS files.
"""

# IMPORTS
import os
import h5py

from typing import Self  # used to type annotate an instance of a class

# Personal imports
from .ServerConnection import SSHMirroredFilesystem


class HDF5Handler:
    """
    To add functionalities when opening an HDF5 file (i.e. .h5 files).
    Main added functionality is the .info() which gives the information of a given HDF5 file (kind of similar to .info() from astropy.io.fits).
    """

    def __init__(self, filename: str, HDF5File: h5py.File, verbose: int, flush: bool) -> None:
        """
        To add functionalities when opening an HDF5 file (i.e. .h5 files).
        Main added functionality is the .info() which gives the information of a given HDF5 file (kind of similar to .info() from astropy.io.fits).
        Need to use the .open() classmethod function on a given HDF5 filepath for the class to work as intended.

        Args:
            filename (str): filename of a given HDF5 file.
            HDF5File (h5py.File): the HDF5 file open in read mode.
            verbose (int): decides the level of the prints.
            flush (bool): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to force the prints or not. 
                Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted exactly when it is 
                called (usually not the case when multiprocessing).
        """
        
        # Non-optional attributes
        self.filename = filename
        self.file = HDF5File
        
        # Miscellaneous attributes
        self.verbose = verbose
        self.flush = flush

    @classmethod
    def open(cls, filepath: str, verbose: int = 0, flush: bool = False) -> Self:
        """
        Opens an HDF5 in read mode using the h5py.File() method. Returns the class itself.

        Args:
            filepath (str): the filepath to the HDF5 file.
            verbose (int, optional): decides the level of the prints. Defaults to 0.
            flush (bool, optional): sets the internal buffer to immediately write the output to it's destination, i.e. it decides to force the prints or not. 
                Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted exactly when it is 
                called (usually not the case when multiprocessing). Defaults to False.

        Raises:
            Exception: when not able to open the HDF5 file.

        Returns:
            Self: adds the new functionalities like .info(level).
        """

        # Initial check
        if not os.path.exists(filepath):
            # Fetching the file from the server.
            if verbose > 0: print(f"\033[37mHDF5 file with filepath {filepath} not found. Connecting to the server...", flush=flush)
            filepath = SSHMirroredFilesystem.remote_to_local(filepath)
            if verbose> 0: print(f"\033[37mFile imported to local\033[0m", flush=flush)
        try:
            h5File = h5py.File(filepath, 'r')

            # Cleaning up if the file was fetched
            SSHMirroredFilesystem.cleanup(verbose)
        except Exception as e:
            raise Exception(f"\033[1;31m'{os.path.basename(filepath)}' not recognised as and HDF5 file. Error: {e}\033[0m")
     
        # Returning the class
        filename = os.path.basename(filepath)
        return cls(filename, h5File, verbose, flush)
            
    def close(self):
        """
        To close the file. From what I was told, it is especially important for HDF5 files.
        """

        self.file.close()

    def info(self, level: int = 10) -> Self:
        """
        To print the general metadata and data information on the HDF5 file. Had the inspiration from the .info() method from astropy.io.fits

        Args:
            level (int, optional): decides the level of the information. 0 only gives the information on the main Datasets and Groups. 1 goes one level further
                down the hierarchy and etc. Defaults to 10.

        Returns:
            Self: returns the instance to allow for chaining if needed.
        """

        # Main metadata setup
        info = [
            "\n" + "=" * 90,
            f"\033[1m\nHDF5 file {self.filename} information.\033[0m",
            f"filename: {self.file.attrs.get('filename', 'No filename')}",
            f"creationDate: {self.file.attrs.get('creationDate', 'No creation date')}",
            f"author: {self.file.attrs.get('author', 'No author')}",
            f"description: {self.file.attrs.get('description', 'No description')}",
            "\n" + "=" * 90 + "\n",
        ] 

        # Exploring the file
        info_extension = self._explore(self.file, level, level)

        # Print creation
        info_list = info + info_extension
        print("\n".join(info_list), flush=self.flush)
        return self 
    
    def _explore(self, group: h5py.File | h5py.Group, max_level: int, level: int) -> list[str]:
        """
        Private instance method to explore a given h5py.File or h5py.Group. Returns the information in a list of strings.

        Args:
            group (h5py.File | h5py.Group): the HDF5 file or one of it's given groups to explore.
            max_level (int): the initial level chosen for the 'level' argument of info(). 
            level (int): the level where we are. Starts from max_level to 0.

        Returns:
            list[str]: file or group information.
        """

        # Level set up
        level -= 1
        rank = max_level - level 
        indentation = " " * 4

        # Exploration setup
        nb_groups = 0
        nb_datasets = 0
        info_groups = []
        info_datasets = []

        # Exploring
        for key, item in group.items():

            # Checking the type
            if isinstance(item, h5py.Dataset):
                info_datasets.extend([
                    f"Dataset {nb_datasets}: {key}  (shape: {item.shape}, dtype: '{item.dtype}')",
                    indentation + f"description: {item.attrs.get('description', 'No description')}",
                ])
                nb_datasets += 1

            elif isinstance(item, h5py.Group):
                info_groups.extend([
                    f"Group {nb_groups}: {key} (member name(s): {', '.join(item.keys())})",
                    indentation + f"description: {item.attrs.get('description', 'No description')}",
                ])
                nb_groups += 1

                # Deeper
                if level > -1: info_groups.extend(self._explore(item, max_level, level))
        
        group_info = [f"\033[1mlvl{rank - 1}: {nb_datasets} dataset(s) and {nb_groups} group(s)\033[0m"]
        print_list = group_info + info_datasets + info_groups + ["-" * 70]
        print_list = [indentation + value for value in print_list]
        return print_list
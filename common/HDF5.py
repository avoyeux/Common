#!/usr/bin/env python3.11
"""
To help when using the HDF5 format. Initial created to have a similar option than the .info() in FITS files.
"""

# IMPORTS
import os
import re
import h5py
import shutil

from typing import Self  # used to type annotate an instance of a class



class HDF5Handler:
    """
    To add functionalities when opening an HDF5 file (i.e. .h5 files).
    Main added functionality is the .info() which gives the information of a given HDF5 file (kind of similar to .info() from astropy.io.fits).
    """

    main_default_keys = ['filename', 'creationDate', 'author', 'description']
    sub_default_keys = ['unit', 'description']

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
        
        # Non-optional instance attributes
        self.filename = filename
        self.file = HDF5File

        # Attributes created at runtime
        self.ansi_escape_pattern = re.compile(r'(\033\[[0-9;]*m)')
        # Placeholders 
        self.max_width: int
        self.all_info: bool
        self.indentation: str
        self.len_indentation: int
        
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
            Self.
        """

        try:
            h5File = h5py.File(filepath, 'r')

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

    def info(self, level: int = 10, indentation: str = "| ", all_info: bool = True) -> Self:
        """
        To print the general metadata and data information on the HDF5 file. Had the inspiration from the .info() method from astropy.io.fits

        Args:
            level (int, optional): decides the max level of the information to be printed out. 
                If 0 only gives the information on the main Datasets and Groups. 1 goes one level further down the hierarchy and vice versa. Defaults to 10.
            indentation (str, optional): sets the indentation added every time you go down a level in the file hierarchy. Defaults to "  ". 
            all_info (bool, optional): choosing to print all the attributes. If False, only the default attributes given by the two class level attributes are 
                printed out. Defaults to True.

        Returns:
            Self: returns the instance to allow for chaining if needed.
        """

        # Get terminal width
        max_width = shutil.get_terminal_size().columns

        # Setting some instance attributes
        self.max_width = max_width
        self.all_info = all_info
        self.indentation = indentation
        self.len_indentation = len(indentation)

        # Title centering
        title = f"HDF5 file '{self.filename}' information"
        title_indentation_len = (max_width // 2) - (len(title) // 2) 
        title_indentation = title_indentation_len * ' ' if title_indentation_len > 0 else ''

        info = [
            "\n" + "=" * max_width + "\n",
            title_indentation + f"\033[1m{title}\n\033[0m",
        ] + [
            string
            for key in HDF5Handler.main_default_keys
            for string in self._reformat_string(str(self.file.attrs.get(key, f'No {key}')), f"\033[92m{key}:\033[0m ")
        ]

        if all_info:
            info += [
                string 
                for key in self.file.attrs.keys()
                if key not in HDF5Handler.main_default_keys
                for string in self._reformat_string(str(self.file.attrs[key]), f"\033[92m{key}:\033[0m ")
            ]
        
        info += ["\n" + "=" * max_width + "\n"]            

        # Exploring the file
        info_extension = self._explore(self.file, level, level)

        # Print creation
        info_list = info + info_extension
        print("\n".join(info_list), flush=self.flush)
        return self 

    def _reformat_string(self, string: str, title: str = '', rank: int = 0, ansi_all: bool = False) -> list[str]:
        """
        To reformat a given string so that you can set a maximum length for each string line (i.e. before each linebreak).
        You can also choose the add the beginning of the string that usually has ANSI code (as it is the title for the print) or have ANSI code in the string by
        setting 'ansi_all'=True. Slower that way, hence the default being 'ansi_all'=False.

        Args:
            string (str): the string to reformat, with or without the title (usually containing ANSI code).
            title (str, optional): the beginning of the string usually with ANSI code. Defaults to ''.
            rank (int, optional): the rank in the hierarchy. The higher the rank, the further down in the file hierarchy the printed information is.
                It is used to also take into account the indentation length. Defaults to 0.
            ansi_all (bool, optional): If true, all the string is checked for ANSI code as it does count as string length but not in the terminal. If only 'title'
                has ANSI code, then keep it False. Defaults to False.

        Returns:
            list[str]: the list of reformatted strings with each having a maximum visualised length (i.e. when printed on a terminal that recognises ANSI code).
            When printing the strings, there should be a linebreak between each list elements.
        """

        # Setup init
        max_string_len = self.max_width - self.len_indentation * rank

        # If ANSI everywhere
        if ansi_all:
            string = title + string

            description = []
            for section in string.split('\n'):
                current_segment = []
                current_len = 0

                for piece in self.ansi_escape_pattern.split(section):
                    if self.ansi_escape_pattern.match(piece):
                        current_segment.append(piece)
                    else:
                        while len(piece) > 0:
                            space_left = max_string_len - current_len

                            if len(piece) <= space_left:
                                current_segment.append(piece)
                                current_len += len(piece)
                                piece = ''
                            else:
                                current_segment.append(piece[:space_left])
                                description.append(''.join(current_segment))
                                piece = piece[space_left:]
                                current_segment = []
                                current_len = 0
            if current_segment or current_len > 0:
                description.append(''.join(current_segment))
        else:
            # Placeholder for ansi beginning string
            without_ansi = re.sub(self.ansi_escape_pattern, '', title)
            string = len(without_ansi) * ' ' + string 

            description = [
            section[i:i + max_string_len].strip()
            for section in string.split('\n')  # keeping the desired linebreaks
            for i in range(0, len(section), max_string_len)
            ]
            description[0] = title + description[0]
        return description

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
        rank = max_level - level

        # Updating the level
        level -= 1

        # Exploration setup
        nb_groups = 0
        nb_datasets = 0
        info_groups = []
        info_datasets = []

        # Exploring
        for key, item in group.items():

            # Information setup
            item_info = [
                string 
                for key in self.sub_default_keys
                for string in self._reformat_string(str(item.attrs.get(key, f'No {key}')), f"\033[92m{key}:\033[0m ", rank)
            ]
            if self.all_info:
                item_info += [
                    string
                    for key in item.attrs.keys()
                    if key not in self.sub_default_keys
                    for string in self._reformat_string(str(item.attrs[key]), f"\033[92m{key}:\033[0m ", rank)
                ]
            
            # Checking the type
            if isinstance(item, h5py.Dataset):
                string_beginning = f"\033[1;94mDataset {nb_datasets}: \033[97m"
                info_datasets.extend(
                    self._reformat_string(
                        f"\033[1;35mDataset {nb_datasets}: \033[97m{key}\033[0m (shape: {item.shape}, dtype: '{item.dtype}')",
                        rank=rank,
                        ansi_all=True
                    ) + item_info
                )
                nb_datasets += 1
            
            elif isinstance(item, h5py.Group):
                info_groups.extend(
                    self._reformat_string(
                        f"\033[1;94mGroup {nb_groups}: \033[97m{key}\033[0m (member name(s): \033[1m{', '.join(item.keys())}\033[0m)",
                        rank=rank,
                        ansi_all=True
                    ) + item_info
                )
                nb_groups += 1

                # DEEPER 
                if level > -1: info_groups.extend(self._explore(item, max_level, level))
        
        group_info = [f"\033[1;90mlvl{rank}: {nb_groups} group(s) and {nb_datasets} dataset(s)\033[0m"]
        print_list = group_info + info_datasets + info_groups + ["-" * (self.max_width - self.len_indentation * rank)]
        if rank != 0: print_list = [self.indentation + value for value in print_list]
        return print_list
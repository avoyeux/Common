#!/usr/bin/env python3.11
"""
To help when using the HDF5 format. Initially created to have a similar option than the .info() in
FITS files.
"""

# IMPORTS
import os
import h5py
import shutil

# IMPORTS sub
from typing import Self

# IMPORTS personal
from .formatting import StringFormatter

# API public
__all__ = ['HDF5Handler']

# todo in .info() need to add the option to be able to omit some group/dataset names or keep some.



class HDF5Handler:
    """
    To add functionalities when opening an HDF5 file (i.e. .h5 files).
    Main added functionality is the .info() which gives the information of a given HDF5 file (kind
    of similar to .info() from astropy.io.fits).
    There is also the .locate() method that gives back the path(s) to an object inside the file
    given the object name.
    """

    def __init__(self, filename: str, HDF5File: h5py.File, verbose: int, flush: bool) -> None:
        """
        To add functionalities when opening an HDF5 file (i.e. .h5 files).
        Main added functionality is the .info() which gives the information of a given HDF5 file 
        (kind of similar to .info() from astropy.io.fits).
        Need to use the .open() classmethod function on a given HDF5 filepath for the class to work
        as intended.

        Args:
            filename (str): filename of a given HDF5 file.
            HDF5File (h5py.File): the HDF5 file open in read mode.
            verbose (int): decides the level of the prints.
            flush (bool): sets the internal buffer to immediately write the output to it's
                destination, i.e. it decides to force the prints or not. Has a negative effect on
                the running efficiency as you are forcing the buffer but makes sure that the print
                is outputted exactly when it is called (usually not the case when multiprocessing).
        """
        
        # ARGUMENTs non-optional
        self.file = HDF5File
        self.filename = filename
        
        # ATTRIBUTEs miscellaneous
        self.verbose = verbose
        self.flush = flush

    @classmethod
    def open(cls, filepath: str, verbose: int = 0, flush: bool = False) -> Self:
        """
        Opens an HDF5 in read mode using the h5py.File() method. Returns the class itself.

        Args:
            filepath (str): the filepath to the HDF5 file.
            verbose (int, optional): decides the level of the prints. Defaults to 0.
            flush (bool, optional): sets the internal buffer to immediately write the output to
                it's destination, i.e. it decides to force the prints or not. Has a negative effect
                on the running efficiency as you are forcing the buffer but makes sure that the
                print is outputted exactly when it is called (usually not the case when
                multiprocessing). Defaults to False.

        Raises:
            Exception: when not able to open the HDF5 file.

        Returns:
            Self.
        """

        try:
            h5File = h5py.File(filepath, 'r')

        except Exception as e:
            raise Exception(
                f"\033[1;31m'{os.path.basename(filepath)}' not recognised as and HDF5 file.\n"
                f"Error: {e}\033[0m"
            )
     
        filename = os.path.basename(filepath)
        return cls(filename, h5File, verbose, flush)
    
    def info(
            self,
            level: int = 10,
            shape: bool = True,
            dtype: bool = True,
            all_info: bool = True,
            disk_size: bool = False,
            indentation: str = '| ',
            memory_size: bool = True,
            compression: bool = False,
        ) -> Self:
        """
        To print the information contained inside the HDF5 file. The precision of the printed
        information will depend on the arguments given.

        Args:
            level (int, optional): decides the max level of the information to be printed out. If 0
                only gives the information on the main Datasets and Groups. 1 goes one level
                further down the hierarchy and vice versa. Defaults to 10.
            shape (bool, optional): deciding to print the shape of each dataset. Defaults to True.
            dtype (bool, optional): deciding to print the dtype of each dataset. Defaults to True.
            all_info (bool, optional): deciding to print all the attributes. If False, only the
                default attributes given by the two class level attributes
                (of HDF5PrintInformation) are printed out. Defaults to True.
            disk_size (bool, optional): deciding to print the disk size of each dataset.
                Defaults to False.
            indentation (str, optional): decides the indentation added every time you go down a
                level. Defaults to '| '.
            memory_size (bool, optional): deciding to print the memory size of each dataset.
                Defaults to True.
            compression (bool, optional): deciding to print the compression type for each dataset.
                Defaults to False.

        Returns:
            Self: the class itself.
        """

        # INITIALIZATION
        print_information = HDF5PrintInformation(
            level=level,
            shape=shape,
            dtype=dtype,
            verbose=self.verbose,
            filename=self.filename,
            all_info=all_info,
            disk_size=disk_size,
            indentation=indentation,
            memory_size=memory_size,
            compression=compression,
            HDF5File=self.file,
        )

        # PRINT
        print(print_information, flush=self.flush)
        return self

    def locate(self, name: str, is_attribute: bool = False) -> list[str]:
        """
        To get the path(s) to an HDF5 Dataset or Group given the name of the object. You can also
        look for an attribute name if you set 'is_attribute' to True.

        Args:
            name (str): the name of the Dataset, Group or attribute if 'is_attribute' set to True.
            is_attribute (bool, optional): When true, the name to locate is a Dataset or Group
                attribute. Defaults to False.

        Returns:
            list[str]: the list of the path(s) corresponding to the given name.
        """

        # PATHS
        found_paths: list[str] = []

        def find_path(path: str, obj):
            """
            Callable needed to use the h5py.visititems(callable) function. The callable is called 
            on all objects in the HDF5 file.

            Args:
                path (str): the name (here path) to the object being visited.
                obj (_type_): the object itself.
            """

            nonlocal found_paths
            if not is_attribute:
                if name in path.split('/'): found_paths.append(path)
            else:
                if name in obj.attrs: found_paths.append(path)

        # VISIT file
        self.file.visititems(find_path)
        return found_paths
    
    def close(self) -> None:
        """
        To close the file. From what I was told, it is especially important for HDF5 files.
        """

        self.file.close()


class HDF5PrintInformation:
    """
    To print the information of a given HDF5 file. The main method is the .info() method.
    """

    main_default_keys = ['filename', 'creationDate', 'author', 'description']
    sub_default_keys = ['unit', 'description']

    def __init__(
            self,
            level: int,
            shape: bool,
            dtype: bool,
            verbose: int,
            filename: str,
            all_info: bool,
            disk_size: bool,
            indentation: str,
            memory_size: bool,
            compression: bool,
            HDF5File: h5py.File,
        ) -> None:
        """
        To print the information of a given HDF5 file. The main method is the .info() method.

        Args:
            level (int): decides the max level of the information to be printed out. If 0 only
                gives the information on the main Datasets and Groups. 1 goes one level further
                down the hierarchy and vice versa.
            verbose (int): decides the level of the prints. The higher the number, the more prints.
            filename (str): filename of the given HDF5 file.
            all_info (bool): choosing to print all the attributes. If False, only the default
                attributes given by the two class level attributes are printed out.
            indentation (str): sets the indentation added every time you go down a level in the
                file hierarchy.
            HDF5File (h5py.File): the HDF5 file open in read mode.
            shape (bool): deciding to print the shape of each dataset when using the .info()
                instance method.
            dtype (bool): deciding to print the dtype of each dataset when using the .info()
                instance method.
            disk_size (bool): deciding to print the disk size of each dataset when using the
                .info() instance method.
            memory_size (bool): deciding to print the memory size of each dataset when using the
                .info() instance method.
            compression (bool): deciding to print the compression type for each dataset when using
                the .info() instance method.
        """

        # ARGUMENTs non-optional
        self.level = level
        self.verbose = verbose
        self.filename = filename
        self.all_info = all_info
        self.indentation = indentation
        self.HDF5File = HDF5File

        # ARGUMENTs optional
        self.shape = shape
        self.dtype = dtype
        self.disk_size = disk_size
        self.memory_size = memory_size
        self.compression = compression

        # ATTRIBUTEs new
        self.max_width = shutil.get_terminal_size().columns
        self.formatter = StringFormatter(
            max_length=self.max_width,
            indentation=self.indentation,
            ansi=True,
        )

    def info(self) -> list[str]:
        """
        To print the general metadata and data information on the HDF5 file. Had the inspiration
        from the .info() method from astropy.io.fits.

        Returns:
            list[str]: the information of the HDF5 file.
        """

        # TITLE centering
        title = f"HDF5 file '{self.filename}' information"
        title_indentation_len = (self.max_width // 2) - (len(title) // 2) 
        title_indentation = title_indentation_len * ' ' if title_indentation_len > 0 else ''

        info = [
            "\n" + "=" * self.max_width + "\n",
            title_indentation + f"\033[1m{title}\n\033[0m",
        ] + [
            string
            for key in self.main_default_keys
            for string in self.formatter.reformat_string(
                string=str(self.HDF5File.attrs.get(key, f'No {key}')),
                prefix=f"\033[92m{key}:\033[0m ",
            )
        ]

        if self.all_info:
            info += [
                string 
                for key in self.HDF5File.attrs.keys()
                if key not in self.main_default_keys
                for string in self.formatter.reformat_string(
                    string=str(self.HDF5File.attrs[key]),
                    prefix=f"\033[92m{key}:\033[0m ",
                )
            ]
        
        info += ["\n" + "=" * self.max_width + "\n"]            

        # EXPLORE file
        info_extension = self._explore(self.HDF5File, self.level)

        # PRINT
        return info + info_extension

    def __str__(self) -> str:
        """
        To return the information of the HDF5 file as a string.

        Returns:
            str: the information of the HDF5 file as a string.
        """
        
        # INFORMATION setup
        info_list = self.info()
        return "\n".join(info_list)
            
    def _explore(self, group: h5py.File | h5py.Group, level: int) -> list[str]:
        """
        Private instance method to explore a given h5py.File or h5py.Group. Returns the information
        in a list of strings.

        Args:
            group (h5py.File | h5py.Group): the HDF5 file or one of it's given groups to explore.
            level (int): the level where we are. Starts from max_level to 0.

        Returns:
            list[str]: file or group information.
        """

        # LEVEL setup
        rank = self.level - level
        level -= 1   # next run update

        # EXPLORATION setup
        nb_groups = 0
        dataset_nb = 0
        info_groups = []
        info_datasets = []

        # EXPLORE
        for key, item in group.items():

            # INFO setup
            item_info = [
                string 
                for key in self.sub_default_keys
                for string in self.formatter.reformat_string(
                    string=str(item.attrs.get(key, f'No {key}')),
                    prefix=f"\033[92m{key}:\033[0m ",
                    rank=rank,
                )
            ]
            if self.all_info:
                item_info += [
                    string
                    for key in item.attrs.keys()
                    if key not in self.sub_default_keys
                    for string in self.formatter.reformat_string(
                        string=str(item.attrs[key]),
                        prefix=f"\033[92m{key}:\033[0m ",
                        rank=rank,
                    )
                ]
            
            # CHECK type
            if isinstance(item, h5py.Dataset):
                info_datasets.extend(
                    self.formatter.reformat_string(
                        string=self._define_dataset(item, key, dataset_nb),
                        rank=rank,
                    ) + item_info
                )
                dataset_nb += 1
            
            elif isinstance(item, h5py.Group):
                info_groups.extend(
                    self.formatter.reformat_string(
                        string=(
                            f"\033[1;94mGroup {nb_groups}: \033[97m{key}\033[0m"
                            f" (member name(s): \033[1m{', '.join(item.keys())}\033[0m)"
                        ),
                        rank=rank,
                    ) + item_info
                )
                nb_groups += 1

                # DEEPER 
                if level > -1: info_groups.extend(self._explore(item, level))
        
        indentation = self.indentation * rank
        group_info = [
            indentation + \
            f"\033[1;90mlvl{rank}: {nb_groups} group(s) and {dataset_nb} dataset(s)\033[0m"
        ]
        print_list = group_info + info_datasets + info_groups + [
            indentation + "-" * (self.max_width - len(self.indentation) * rank)
        ]
        return print_list
    
    def _define_dataset(self, dataset: h5py.Dataset, dataset_name: str, dataset_nb: int) -> str:
        """
        To define the string to print for a given dataset.

        Args:
            dataset (h5py.Dataset): the dataset to define.
            dataset_name (str): the name of the dataset.
            dataset_nb (int): the id inside the group corresponding to the dataset.

        Returns:
            str: the definition of the dataset.
        """

        string = f"\033[1;35mDataset {dataset_nb}: \033[97m{dataset_name}\033[0m ("

        if self.shape:
            shape = dataset.shape
            if shape != ():
                string += f"shape: {shape}, "
            else:
                string += f"value: {dataset[...]}, "

        if self.dtype: string += f"dtype: '{dataset.dtype}', "

        if self.compression:
            compression = dataset.compression
            compression_lvl = dataset.compression_opts

            if compression is None:
                string += f"compression: None, "
            else:
                string += f"compression(lvl{compression_lvl}): {compression}, "
            
        if self.disk_size:
            string += (
                f"disk size: {self.formatter.nbytes_to_human(dataset.id.get_storage_size())}, "
            )

        if self.memory_size: 
            string += f"memory size: {self.formatter.nbytes_to_human(dataset.nbytes)}, "

        # CLEANUP
        cleaned_up = string.rstrip(" (").rstrip(", ") + ")"
        return cleaned_up

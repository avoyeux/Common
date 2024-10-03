#!/usr/bin/env python3.11
"""
Has functions that help me when multiprocessing.
"""

# Imports
import typing

import numpy as np
import multiprocessing as mp

# Sub imports
import multiprocessing.shared_memory


class MultiProcessing:
    """
    Useful when using the multiprocessing module.
    """

    def __init__(
            self,
            input_data: list | np.ndarray,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory: bool = False,
        ):
        #TODO: to do multiprocessing automatically when trying to save time.

        # Arguments
        self.input_data = input_data
        self.function = function
        self.function_kwargs = function_kwargs
        self.processes = processes
        self.shared_memory = shared_memory
        
    @classmethod
    def multiprocess(
            cls,
            input_data: list | np.ndarray,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory: bool = False,
        ):
        #TODO: to get the multiprocessing results directly 

        instance = cls(
            input_data=input_data,
            function=function,
            function_kwargs=function_kwargs,
            processes=processes,
            shared_memory=shared_memory,
        )
    
    def multiprocess_sub(self):
        #TODO: to do the multiprocessing
        pass

    @staticmethod
    def pool_indexes(data_length: int, nb_processes: int) -> list[tuple[int, int]]:
        """
        Gives out a list of tuples with the start and last data index for each process.

        Args:
            data_length (int): the length of the data that you want to multiprocess.
            nb_processes (int): the number or processes you want to run. If higher than data_length then data_length is used.

        Returns:
            list[tuple[int, int]]: the list of the start and end indexes for each process.
        """
        
        if data_length > nb_processes:

            # Step per process
            step = data_length // nb_processes
            leftover = data_length % nb_processes

            return [((step * i) + min(i, leftover), step * (i + 1) + min(i + 1, leftover) - 1) for i in range(nb_processes)]
        else:
            return[(i, i) for i in range(data_length)]
    
    @staticmethod
    def shared_memory(data: np.ndarray) -> tuple[mp.shared_memory.SharedMemory, dict[str, any]]:
        """
        Creating a shared memory space given an input np.ndarray.

        Args:
            data (np.ndarray): data array that you want to create a shared memory object for.

        Returns:
            tuple[SharedMemory, dict[str, any]]: information needed to access the shared memory object.
        """

        # Initialisations
        shm = mp.shared_memory.SharedMemory(create=True, size=data.nbytes)
        info = {
            'name': shm.name,
            'shape': data.shape,
            'dtype': data.dtype,
        }
        shared_array = np.ndarray(info['shape'], dtype=info['dtype'], buffer=shm.buf)
        np.copyto(shared_array, data)
        shm.close()
        return shm, info
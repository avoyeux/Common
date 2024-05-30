"""Has functions that help me when multiprocessing.
"""

import numpy as np
from typeguard import typechecked
from multiprocessing.shared_memory import SharedMemory


class MultiProcessing:
    """Some functions that are useful when multiprocessing.
    """

    @typechecked
    @staticmethod
    def Pool_indexes(data_length: int, nb_processes: int = 4) -> list[tuple[int, int]]:
        """Gives out a list of tuples with the start and last data index for each process.

        Args:
            data_length (int): the length of the data that you want to multiprocess.
            nb_processes (int, optional: the number or processes you want to run. If higher than data_length then it becomes data_length. Defaults to 4.

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
    
    @typechecked
    @staticmethod
    def Shared_memory(self, data: np.ndarray) -> tuple[SharedMemory, dict[str, any]]:
        """Creating a shared memory space given an input np.ndarray.

        Args:
            data (np.ndarray): data array that you want to create a shared memory object for.

        Returns:
            tuple[SharedMemory, dict[str, any]]: information needed to access the shared memory object.
        """

        # Initialisations
        shm = SharedMemory(create=True, size=data.nbytes)
        info = {
            'shm.name': shm.name,
            'data.shape': data.shape,
            'data.dtype': data.dtype,
        }
        shared_array = np.ndarray(info['data.shape'], dtype=info['data.dtype'], buffer=shm.buf)
        np.copyto(shared_array, data)
        shm.close()
        return shm, info
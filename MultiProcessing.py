"""Has functions that help me when multiprocessing
"""

from numpy import ceil
from typeguard import typechecked


class MultiProcessing:
    """Some functions that are useful when multiprocessing.
    """

    @typechecked
    @staticmethod
    def Pool_indexes(data_length: int, nb_processes: int = 4) -> list[tuple[int, int]]:
        """
        Gives out a list of tuples with the start and last data index for each process.
        """

        # Step per process
        step = int(ceil(data_length / nb_processes))
        return [(step * i, step * (i + 1) - 1 if i != nb_processes - 1 else data_length - 1) for i in range(nb_processes)]
#!/usr/bin/env python3.11
"""
Has functions that help me when multiprocessing.
"""

# Imports
import typing

import numpy as np
import multiprocessing as mp

# Sub imports
import multiprocessing.queues
import multiprocessing.shared_memory

# Public classes
__all__ = ['MultiProcessing']



class MultiProcessing:
    """
    Useful when using the multiprocessing module.
    There are so many similar functions just to make sure that no if statements are incorporated in the for loops.
    """

    @staticmethod
    def multiprocessing(
            input_data: list | np.ndarray | dict[str, any],
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory_input: bool = False,
            function_input_shared_memory: bool = False,
            identifier: bool = False,
            while_True: bool = False,        
    ) -> list:
        """
        To multiprocess a given function with the corresponding data and keyword arguments. The input function needs to have the data argument as the first 
        function argument.
        You can choose to multiprocess using with or without a while loop (c.f. 'while_True' argument) and even with a shared memory information dictionary 
        (gotten from cls.shared_memory) as the 'input_data' value to the class and/or the multiprocessed function. You can also decide to input the data 
        identifier in the function to be multiprocessed so that you know which part of the data is being inputted.

        Args:
            input_data (list | np.ndarray | dict[str, any]): the data to be multiprocessed. It can also be the data shared memory information 
                dictionary gotten cls.shared_memory(). 
            function (typing.Callable[..., any]): the function to be multiprocessed.
            function_kwargs (dict[str, any]): the multiprocessed function's keyword arguments.
            processes (int): the number of processes used in the multiprocessing.
            shared_memory_input (bool, optional): if the input data is actually a shared memory information dictionary gotten from cls.shared_memory. In this
                case, or when function_shared_memory is set to true, all the multiprocessed function take the totality of the data shared memory pointer information
                as the data argument. Hence, when set to True, it is usually advised to also set 'identifier' to True. Defaults to False.
            function_shared_memory (bool, optional): deciding to input inside the function to be multiprocessed a shared memory object gotten by 
                preprocessing the data with cls.shared_memory(). In this case, then the same data than the input is given to each child process. In that case, 
                it is advised to set 'identifier' to True (c.f. 'identifier'). Defaults to False.
            identifier (bool, optional): to add an int identifier (to identify which data index or section is being processed) as an input argument
                right after the data argument. Defaults to False.
            while_True (bool, optional): deciding to use a while True loop for the multiprocessing. Should be set to False when the function to be 
                multiprocessed benefits from doing calculations on larger data sets, e.g. doing ndarray multiplications as does run directly in C.
                User need to pay attention as when while_True is True, the resulting outputted list has the "shape" (nb_processes, corresponding section size, ...).
                When kept False, the resulting list will have the same "shape" than the initial data, i.e. len(outputted_list) == len(input_data).
                Defaults to False.

        Returns:
            list: a list of the results. If 'while_True' is set to True, the "shape" of the list is the same than for the input. Else, the "shape" of 
                the list will be (nb_processes, corresponding data section size, ...).
        """

        instance = MultiprocessingUtils(
            input_data=input_data,
            function=function,
            function_kwargs=function_kwargs,
            processes=processes,
            shared_memory=shared_memory_input,
            function_shared_memory=function_input_shared_memory,
            identifier=identifier,
            while_True=while_True,
        )
        return instance.multiprocess_choices()
        
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
    

class MultiprocessingUtils:
    """
    To store some private functions.
    """

    def __init__(
            self,
            input_data: list | np.ndarray | dict[str, any],
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory: bool,
            function_shared_memory: bool,
            identifier: bool,
            while_True: bool, 
        ) -> None:
        """
        To multiprocess a given function with the corresponding data and keyword arguments. The input function needs to have the data argument as the first 
        function argument.
        You can choose to multiprocess using with or without a while loop (c.f. 'while_True' argument) and even with a shared memory information dictionary 
        (gotten from cls.shared_memory) as the 'input_data' value to the class and/or the multiprocessed function. You can also decide to input the data 
        identifier in the function to be multiprocessed so that you know which part of the data is being inputted.

        Args:
            input_data (list | np.ndarray | dict[str, any]): the data to be multiprocessed. It can also be the data shared memory information 
                dictionary gotten cls.shared_memory(). 
            function (typing.Callable[..., any]): the function to be multiprocessed.
            function_kwargs (dict[str, any]): the multiprocessed function's keyword arguments.
            processes (int): the number of processes used in the multiprocessing.
            shared_memory (bool): if the input data is actually a shared memory information dictionary gotten from cls.shared_memory. In this
                case, or when function_shared_memory is set to true, all the multiprocessed function take the totality of the data shared memory pointer information
                as the data argument. Hence, when set to True, it is usually advised to also set 'identifier' to True.
            function_shared_memory (bool): deciding to input inside the function to be multiprocessed a shared memory object gotten by 
                preprocessing the data with cls.shared_memory(). In this case, then the same data than the input is given to each child process. In that case, 
                it is advised to set 'identifier' to True (c.f. 'identifier').
            identifier (bool): to add an int identifier (to identify which data index or section is being processed) as an input argument
                right after the data argument.
            while_True (bool): deciding to use a while True loop for the multiprocessing. Should be set to False when the function to be 
                multiprocessed benefits from doing calculations on larger data sets, e.g. doing ndarray multiplications as does run directly in C.
                User need to pay attention as when while_True is True, the resulting outputted list has the "shape" (nb_processes, corresponding section size, ...).
                When kept False, the resulting list will have the same "shape" than the initial data, i.e. len(outputted_list) == len(input_data).

        Returns:
            list: a list of the results. If 'while_True' is set to True, the "shape" of the list is the same than for the input. Else, the "shape" of 
                the list will be (nb_processes, corresponding data section size, ...).
        """

        # Arguments
        self.input_data = input_data
        self.function = function
        self.function_kwargs = function_kwargs
        self.processes = processes
        self.shared_memory = shared_memory
        self.function_shared_memory = function_shared_memory
        self.identifier = identifier
        self.while_true = while_True

        # Created arguments
        self.data_len = self.input_data['shape'][0] if shared_memory else len(self.input_data) 
        self.nb_processes = min(self.data_len, processes)

    def multiprocess_choices(self) -> list:
        """
        To choose which multiprocessing functions are going to be used (i.e. a while True loop or by section).

        Returns:
            list: a list of the results. If 'while_True' is set to True, the "shape" of the list is the same than for the input. Else, the "shape" of 
                the list will be (nb_processes, corresponding data section size, ...).
        """

        # Basic setup
        shm = None
        manager = mp.Manager()
        output_queue = manager.Queue()

        # Shared memory setup
        if not self.shared_memory and self.function_shared_memory:
            if not isinstance(self.input_data, np.ndarray): self.input_data = np.array(self.input_data)
            shm, self.input_data = MultiProcessing.shared_memory(self.input_data)
        elif self.shared_memory and (not self.function_shared_memory):
            # Open shared memory
            shm = mp.shared_memory.SharedMemory(name=self.input_data['name'])
            self.input_data = np.ndarray(shape=self.input_data['shape'], dtype=self.input_data['dtype'], buffer=shm.buf)

        # Choose method
        if self.while_true:
            input_queue = manager.Queue()
            if isinstance(self.input_data, dict):
                self._multiprocess_while_all_data(input_queue, output_queue)
            else:
                self._multiprocess_while(input_queue, output_queue)

            # Get results
            results = [None] * self.data_len
            while not output_queue.empty():
                identifier, result = output_queue.get()
                results[identifier] = result
        else:
            if isinstance(self.input_data, dict):
                self._multiprocess_indexes_all_data(output_queue)
            else:
                self._multiprocess_indexes(output_queue)

            # Get results
            results = [None] * self.nb_processes
            while not output_queue.empty():
                identifier, result = output_queue.get()
                results[identifier] = result
        
        if shm is not None: 
            if not self.shared_memory and self.function_shared_memory:
                shm.unlink()
            else:
                shm.close()
        return results
    
    def _multiprocess_indexes_all_data(self, output_queue: mp.queues.Queue) -> None:
        """
        Multiprocessing by using sections of the data to leverage as much as possible operations written in C (e.g. np.ndarray multiplications).
        In this case, all the data is given as input to each process (when dealing with shared memory object information saved inside a dictionary).

        Args:
            output_queue (mp.queues.Queue): the results gotten from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes
        indexes = MultiProcessing.pool_indexes(self.data_len, self.nb_processes)

        if self.identifier:
            for i, index in enumerate(indexes):
                p = mp.Process(
                    target=self._multiprocessing_indexes_sub_with_indexes,
                    kwargs={
                        'data': self.input_data,
                        'function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'output_queue': output_queue,
                        'identifier': i,
                        'index': index,
                    },
                )
                p.start()
                processes[i] = p
        else:
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_indexes_sub_without_indexes,
                    kwargs={
                        'data': self.input_data,
                        'function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'output_queue': output_queue,
                        'identifier': i,
                    },
                )
                p.start()
                processes[i] = p
        for p in processes: p.join()

    def _multiprocess_indexes(self, output_queue: mp.queues.Queue) -> None:
        """
        Multiprocessing by using sections of the data to leverage as much as possible operations written in C (e.g. np.ndarray multiplications).

        Args:
            output_queue (mp.queues.Queue): the results gotten from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes
        indexes = MultiProcessing.pool_indexes(self.data_len, self.nb_processes)

        if self.identifier:
            for i, index in enumerate(indexes):
                p = mp.Process(
                    target=self._multiprocessing_indexes_sub_with_indexes,
                    kwargs={
                        'data': self.input_data[index[0]:index[1] + 1],
                        'function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'output_queue': output_queue,
                        'identifier': i,
                        'index': index,
                    },
                )
                p.start()
                processes[i] = p
        else:
            for i, index in enumerate(indexes):
                p = mp.Process(
                    target=self._multiprocessing_indexes_sub_without_indexes,
                    kwargs={
                        'data': self.input_data[index[0]:index[1] + 1],
                        'function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'output_queue': output_queue,
                        'identifier': i,
                    },
                )
                p.start()
                processes[i] = p
        for p in processes: p.join()

    @staticmethod
    def _multiprocessing_indexes_sub_without_indexes(
            data: any,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
    ) -> None:
        """
        Multiprocessing a function.

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """
        
        result = function(data, **function_kwargs)
        output_queue.put((identifier, result))

    @staticmethod
    def _multiprocessing_indexes_sub_with_indexes(
            data: any,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
            index: tuple[int, int],
    ) -> None:
        """
        Multiprocessing a function. In this case, the identifier is used as the second argument to the function to 
        be multiprocessed.

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
            index (tuple[int, int]): the indexes for the section of the data that is being multiprocessed.
        """
        result = function(data, index, **function_kwargs)
        output_queue.put((identifier, result))

    def _multiprocess_while_all_data(self, input_queue: mp.queues.Queue, output_queue: mp.queues.Queue) -> None:
        """
        Multiprocessing all the data using a while loop. This means that the function to be multiprocessed should only take 
        one index of the main data as the first function argument. 
        For this 'all_data' function, all the initial data is given as an argument to the function to be multiprocessed through
        a dictionary with the necessary information to open the corresponding shared memory object (cf. MultiProcessing.shared_memory()).

        Args:
            input_queue (mp.queues.Queue): has the index identifier of the data is going to be processed.
            output_queue (mp.queues.Queue): to save the identifier and the corresponding result from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes
        for i in range(self.data_len): input_queue.put(i)
        for _ in processes: input_queue.put(None)

        # Run
        if self.identifier:
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_with_indexes_all_data,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'data': self.input_data,
                        'input_queue': input_queue,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        else:
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_without_indexes_all_data,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'data': self.input_data,
                        'input_queue': input_queue,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        for p in processes: p.join()

    def _multiprocess_while(self, input_queue: mp.queues.Queue, output_queue: mp.queues.Queue) -> None:
        """
        Multiprocessing all the data using a while loop. This means that the function to be multiprocessed should only take 
        one index of the main data as the first function argument. 

        Args:
            input_queue (mp.queues.Queue): an empty queue to be populated.
            output_queue (mp.queues.Queue): to save the identifier and the corresponding result from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes
        for i in range(self.data_len): input_queue.put((i, self.input_data[i]))
        for _ in processes: input_queue.put(None)

        # Run
        if self.identifier:
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_with_indexes,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'input_queue': input_queue,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        else:
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_without_indexes,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'input_queue': input_queue,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        for p in processes: p.join()
    
    @staticmethod
    def _multiprocessing_while_sub_with_indexes_all_data(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            data: dict[str, any],
            input_queue: mp.queues.Queue,
            output_queue: mp.queues.Queue,
        ) -> None:
        """
        To run a process in a while loop given an input and output queue. The data is accessed through the information of a shared memory object.
        Furthermore, the identifier is also given to the function to be multiprocessed as the second argument of the function.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            data (dict[str, any]): the shared memory information to point to the data.
            input_queue (mp.queues.Queue): to get the identifier to know what part of the data is being processed.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """
        
        # Run
        while True:
            # Arguments
            identifier = input_queue.get()
            if identifier is None: return

            # Get result
            result = input_function(data, identifier, **function_kwargs)
            # Save result
            output_queue.put((identifier, result))            

    @staticmethod
    def _multiprocessing_while_sub_without_indexes_all_data(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            data: dict[str, any],
            input_queue: mp.queues.Queue,
            output_queue: mp.queues.Queue,
        ) -> None:
        """
        To run a process in a while loop given an input and output queue. The data is accessed through the information of a shared memory object.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            data (dict[str, any]): the shared memory information to point to the data.
            input_queue (mp.queues.Queue): to get the identifier to know what part of the data is being processed.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """
        
        # Run
        while True:
            # Arguments
            identifier = input_queue.get()
            if identifier is None: return

            # Get result
            result = input_function(data, **function_kwargs)
            # Save result
            output_queue.put((identifier, result))

    @staticmethod
    def _multiprocessing_while_sub_with_indexes(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            input_queue: mp.queues.Queue,
            output_queue: mp.queues.Queue,
        ) -> None:
        """
        To run a process in a while loop given an input and output queue.
        In this case, the second argument of the function to be multiprocessed is the identifier for which part of the data is 
        being processed.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            input_queue (mp.queues.Queue): to get the identifier and the corresponding data to be multiprocessed.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """
        
        # Run
        while True:
            # Arguments
            args = input_queue.get()
            if args is None: return
            identifier, data = args

            # Get result
            result = input_function(data, identifier, **function_kwargs)
            # Save result
            output_queue.put((identifier, result))            

    @staticmethod
    def _multiprocessing_while_sub_without_indexes(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            input_queue: mp.queues.Queue,
            output_queue: mp.queues.Queue,
        ) -> None:
        """
        To run a process in a while loop given an input and output queue.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            input_queue (mp.queues.Queue): to get the identifier and the corresponding data to be multiprocessed.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """
        
        # Run
        while True:
            # Arguments
            args = input_queue.get()
            if args is None: return
            identifier, data = args

            # Get result
            result = input_function(data, **function_kwargs)
            # Save result
            output_queue.put((identifier, result))                
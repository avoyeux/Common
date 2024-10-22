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
import multiprocessing.managers
import multiprocessing.shared_memory

# Public classes
__all__ = ['MultiProcessing']


class SharedMemoryList(list):
    """
    Private class to be able to use .close() and .unlink() directly on a list of shared memory objects.
    It inherits from the list class so that the instance itself it a list.
    """

    def __init__(self, shm: list[mp.shared_memory.SharedMemory]) -> None:
        """
        To be able to use .close() and .unlink() directly on a list of shared memory objects.

        Args:
            shm (list[mp.shared_memory.SharedMemory]): the list of the shared memory objects.
        """

        super().__init__(shm)
    
    def close(self) -> None:
        """
        Calls the .close() method on each shared memory object inside the list.
        """

        for memory in self: memory.close()
    
    def unlink(self) -> None:
        """
        Calls the .unlink() method on each shared memory object inside the list.
        """

        for memory in self: memory.unlink()


class MultiProcessing:
    """
    Useful when using the multiprocessing module.
    There are so many similar functions just to make sure that no if statements are incorporated in the for loops.
    """
    #TODO: I need to add an option where the inputted subprocess data is the whole inputted data.
    @staticmethod
    def multiprocessing(
            input_data: list | np.ndarray | dict[str, any],
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory_input: bool = False,
            create_shared_memory: bool = False,
            multiple_shared_memory: bool = False,
            transfer_all_data: bool = False,
            identifier: bool = False,
            while_True: bool = False,       
            verbose: int = 0, 
        ) -> list:
        """
        To multiprocess a given function with the corresponding data and keyword arguments. The input function needs to have the data argument as the first 
        function argument.
        You can choose to multiprocess using with or without a while loop (c.f. 'while_True' argument). You also have shared memory choices like if you want
        each subprocess to have a view of a buffer of the data or you want the input to the multiprocessing to already be a shared_memory object (gotten with
        the cls.create_shared_memory method). 

        Args:
            input_data (list | np.ndarray | dict[str, any]): the data to be multiprocessed. It can also be the data shared memory information 
                dictionary gotten from cls.create_shared_memory(). 
            function (typing.Callable[..., any]): the function to be multiprocessed.
            function_kwargs (dict[str, any]): the multiprocessed function's keyword arguments.
            processes (int): the number of processes used in the multiprocessing.
            shared_memory_input (bool, optional): if the input data is actually a shared memory information dictionary gotten from cls.shared_memory. Defaults
                to False.
            create_shared_memory (bool, optional): deciding to create a shared memory object for the data. In this case, the data given to the subprocesses
                is the view to the buffer of the inputted data. Defaults to False.
            multiple_shared_memory (bool, optional): deciding to create a list of shared memory objects as opposed to only one. Useful when the inputted data 
                is a list of ndarrays of different shapes. If the inputted data is a list and 'multiple_shared_memory' is set to False, then the inputted data
                will be converted to an unique np.ndarray. If not possible, then 'multiple_shared_memory' will be set to True. Defaults to False.
            transfer_all_data (bool, optional): choosing to transfer all the inputted data to every subprocess. Defaults to False.
            identifier (bool, optional): to add an int identifier (to identify which data index or section is being processed) as an input argument
                right after the data argument. Defaults to False.
            while_True (bool, optional): deciding to use a while True loop for the multiprocessing. Should be set to False when the function to be 
                multiprocessed benefits from doing calculations on larger data sets, e.g. doing ndarray multiplications as does run directly in C.
                User need to pay attention as when while_True is True, the resulting outputted list has the "shape" (nb_processes, corresponding section size, ...).
                When kept False, the resulting list will have the same "shape" than the initial data, i.e. len(outputted_list) == len(input_data).
                Defaults to False.
            verbose (int, optional): the higher the value, the more prints will be outputted. When 0, no prints. Defaults to 0.

        Returns:
            list: a list of the results. If 'while_True' is set to True, the "shape" of the list is the same than for the input. Else, the "shape" of 
                the list will be (nb_processes, corresponding data section size, ...).
        """

        instance = MultiProcessingUtils(
            input_data=input_data,
            function=function,
            function_kwargs=function_kwargs,
            processes=processes,
            shared_memory_input=shared_memory_input,
            create_shared_memory=create_shared_memory,
            multiple_shared_memory=multiple_shared_memory,
            transfer_all_data=transfer_all_data,
            identifier=identifier,
            while_True=while_True,
            verbose=verbose,
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
    def create_shared_memory(
            data: np.ndarray | list[np.ndarray],
            multiple: bool = False,
            verbose: int = 1,
        ) -> tuple[mp.shared_memory.SharedMemory | list[mp.shared_memory.SharedMemory], dict[str, any]]:
        """
        Creating shared memory object(s) given an input ndarray or list of ndarray.  

        Args:
            data (np.ndarray | list[np.ndarray]): data array that you want to create shared memory object(s) for.
            multiple (bool, optional): choosing to create multiple shared memories or just one. Defaults to False.
            verbose(int, optional): choosing to print a message when 'multiple' is set to False and the data can't be converted to an ndarray.
                Defaults to 1.

        Returns:
            tuple[mp.shared_memory.SharedMemory | list[mp.shared_memory.SharedMemory], dict[str, any]]: information needed to access the shared memory object(s).
        """

        if multiple:
            shm, info = MultiProcessingUtils.shared_memory_multiple(data)

        elif isinstance(data, list):
            try: 
                data = np.ndarray(data)
            except Exception:
                if verbose > 0: print("\033[37mShared_memory function couldn't change data to an ndarray. Creating multiple shared memories.\033[0m")
                shm, info = MultiProcessingUtils.shared_memory_multiple(data)
        else:
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
    
    @staticmethod
    def open_shared_memory(
            data_info: dict[str, any]
        ) -> tuple[mp.shared_memory.SharedMemory, np.ndarray] | tuple[SharedMemoryList, list[np.ndarray]]:
        """
        To open a shared memory multiprocessing object using the MultiProcessing.create_shared_memory method shared memory information.

        Args:
            data_info (dict[str, any]): the shared memory information gotten from MultiProcessing.create_shared_memory() method.

        Returns:
            tuple[mp.shared_memory.SharedMemory, np.ndarray] | tuple[SharedMemoryList, list[np.ndarray]]: the shared memory object buffer
                and the corresponding np.ndarray view of the buffer. Depending on the information given as input, the output can be lists 
                of the buffers and buffer views or just one buffer with the corresponding data view. Regardless, .close() or .unlink() can 
                directly be used on the buffer(s).
        """

        if isinstance(data_info['name'], str):
            shm = mp.shared_memory.SharedMemory(name=data_info['name'])
            data = np.ndarray(shape=data_info['shape'], dtype=data_info['dtype'], buffer=shm.buf)
            return shm, data
        
        else:
            data_len = len(data_info['name'])
            shm = [None] * data_len
            data = [None] * data_len

            for i in range(data_len):
                shm[i] = mp.shared_memory.SharedMemory(name=data_info['name'][i])
                data[i] = np.ndarray(
                    shape=data_info['shape'][i],
                    dtype=data_info['dtype'][i],
                    buffer=shm[i].buf,
                )
            return SharedMemoryList(shm), data


class MultiProcessingUtils:
    """
    To store some private functions.
    """

    def __init__(
            self,
            input_data: list | np.ndarray | dict[str, any],
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            processes: int,
            shared_memory_input: bool,
            create_shared_memory: bool,
            multiple_shared_memory: bool,
            transfer_all_data: bool,
            identifier: bool,
            while_True: bool,    
            verbose: int,
        ) -> None:
        """
        To multiprocess a given function with the corresponding data and keyword arguments. The input function needs to have the data argument as the first 
        function argument.
        You can choose to multiprocess using with or without a while loop (c.f. 'while_True' argument). You also have shared memory choices like if you want
        each subprocess to have a view of a buffer of the data or you want the input to the multiprocessing to already be a shared_memory object (gotten with
        the cls.create_shared_memory method). 

        Args:
            input_data (list | np.ndarray | dict[str, any]): the data to be multiprocessed. It can also be the data shared memory information 
                dictionary gotten from cls.create_shared_memory(). 
            function (typing.Callable[..., any]): the function to be multiprocessed.
            function_kwargs (dict[str, any]): the multiprocessed function's keyword arguments.
            processes (int): the number of processes used in the multiprocessing.
            shared_memory_input (bool): if the input data is actually a shared memory information dictionary gotten from cls.shared_memory.
            create_shared_memory (bool): deciding to create a shared memory object for the data. In this case, the data given to the subprocesses
                is the view to the buffer of the inputted data.
            multiple_shared_memory (bool): deciding to create a list of shared memory objects as opposed to only one. Useful when the inputted data 
                is a list of ndarrays of different shapes. If the inputted data is a list and 'multiple_shared_memory' is set to False, then the inputted data
                will be converted to an unique np.ndarray. If not possible, then 'multiple_shared_memory' will be set to True.
            transfer_all_data (bool): choosing to transfer all the inputted data to each subprocess.
            identifier (bool): to add an int identifier (to identify which data index or section is being processed) as an input argument
                right after the data argument. 
            while_True (bool): deciding to use a while True loop for the multiprocessing. Should be set to False when the function to be 
                multiprocessed benefits from doing calculations on larger data sets, e.g. doing ndarray multiplications as does run directly in C.
                User need to pay attention as when while_True is True, the resulting outputted list has the "shape" (nb_processes, corresponding section size, ...).
                When kept False, the resulting list will have the same "shape" than the initial data, i.e. len(outputted_list) == len(input_data).
            verbose (int): the higher the value, the more prints will be outputted. When 0, no prints.

        Returns:
            list: a list of the results. If 'while_True' is set to True, the "shape" of the list is the same than for the input. Else, the "shape" of 
                the list will be (nb_processes, corresponding data section size, ...).
        """

        # Arguments
        self.input_data = input_data
        self.function = function
        self.function_kwargs = function_kwargs
        self.processes = processes
        self.shared_memory_input = shared_memory_input
        self.create_shared_memory = create_shared_memory
        self.multiple_shared_memory = multiple_shared_memory
        self.transfer_all_data = transfer_all_data
        self.identifier = identifier
        self.while_true = while_True
        self.verbose = verbose

        # Created arguments
        self.data_len: int = self.input_data['shape'][0] if shared_memory_input else len(self.input_data) 
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
        if self.create_shared_memory:
            if not self.shared_memory_input: 
                shm, self.input_data = MultiProcessing.create_shared_memory(
                    data=self.input_data,
                    multiple=self.multiple_shared_memory,
                    verbose=self.verbose,
                )
        elif self.shared_memory_input: 
            shm, self.input_data = MultiProcessing.open_shared_memory(self.input_data)


        # Choose method
        if self.while_true:
            shared_value = manager.Value('i', 0)
            print('shared value created', flush=True)
            if self.transfer_all_data:
                self._multiprocess_while_all_data(shared_value, output_queue)
            else:
                self._multiprocess_while(shared_value, output_queue)

            results = [None] * self.data_len
        else:
            if self.transfer_all_data: 
                self._multiprocess_indexes_all_data(output_queue)
            else:
                self._multiprocess_indexes(output_queue)

            results = [None] * self.nb_processes
        
        # Get results
        while not output_queue.empty():
            identifier, result = output_queue.get()
            results[identifier] = result
        
        # Manage buffer(s)
        if shm is not None:
            shm.close()
            if self.create_shared_memory and not self.shared_memory_input: shm.unlink()
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
                    target=self._multiprocessing_indexes_sub_with_indexes_all_data,
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
                    target=self._multiprocessing_indexes_sub_without_indexes_all_data,
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

        if isinstance(self.input_data, dict) and ('name' in self.input_data.keys()):
            if self.identifier:
                for i, index in enumerate(indexes):
                    p = mp.Process(
                        target=self._multiprocessing_indexes_sub_with_indexes_dict,
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
                for i, index in enumerate(indexes):
                    p = mp.Process(
                        target=self._multiprocessing_indexes_sub_without_indexes_dict,
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
        else:

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
    def _multiprocessing_indexes_sub_without_indexes_all_data(
            data: any,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
    ) -> None:
        """
        Multiprocessing a function. #TODO: change docstring

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """
        
        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)

        result = function(data, **function_kwargs)  #TODO: this won't work for a list[np.ndarray]...
        output_queue.put((identifier, result))

        if shm is not None: shm.close()

    @staticmethod
    def _multiprocessing_indexes_sub_with_indexes_all_data(
            data: any,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
            index: tuple[int, int],
    ) -> None:
        """
        Multiprocessing a function. #TODO: change docstring

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """

        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)
        
        result = function(data, index,  **function_kwargs)  #TODO: this won't work for a list[np.ndarray]...
        output_queue.put((identifier, result))

        if shm is not None: shm.close()

    @staticmethod
    def _multiprocessing_indexes_sub_without_indexes(
            data: any,
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
    ) -> None:
        """
        Multiprocessing a function. #TODO: change docstring

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """
        
        result = function(data, **function_kwargs)  #TODO: this won't work for a list[np.ndarray]...
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
        Multiprocessing a function. #TODO: change docstring

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """
        
        result = function(data, index, **function_kwargs)  #TODO: this won't work for a list[np.ndarray]...
        output_queue.put((identifier, result))

    @staticmethod
    def _multiprocessing_indexes_sub_without_indexes_dict(
            data: dict[str, any],
            function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            output_queue: mp.queues.Queue,
            identifier: int,
            index: tuple[int, int],
    ) -> None:
        """
        Multiprocessing a function. #TODO: change docstring

        Args:
            data (any): the data to be inputted in the function.
            function (typing.Callable[..., any]): the function used in the multiprocessing.
            function_kwargs (dict[str, any]): the function keyword arguments.
            output_queue (mp.queues.Queue): to get the results outside the function.
            identifier (int): the identifier to know which section of the main data is being multiprocessed.
        """

        shm, data = MultiProcessing.open_shared_memory(data)
        
        result = function(data[index[0]:index[1] + 1], **function_kwargs)  #TODO: this won't work for a list[np.ndarray]...
        output_queue.put((identifier, result))

        shm.close()

    @staticmethod
    def _multiprocessing_indexes_sub_with_indexes_dict(
            data: dict[str, any],
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
        
        shm, data = MultiProcessing.open_shared_memory(data)

        result = function(data[index[0]:index[1] + 1], index, **function_kwargs) #TODO: this won't work for a list[np.ndarray] 
        output_queue.put((identifier, result))

        shm.close()

    def _multiprocess_while_all_data(self, shared_value: mp.managers.ValueProxy, output_queue: mp.queues.Queue) -> None:
        """ #TODO:change docstring
        Multiprocessing all the data using a while loop. This means that the function to be multiprocessed should only take 
        one index of the main data as the first function argument. 
        For this 'all_data' function, all the initial data is given as an argument to the function to be multiprocessed through
        a dictionary with the necessary information to open the corresponding shared memory object (cf. MultiProcessing.create_shared_memory()).

        Args:
            input_queue (mp.queues.Queue): has the index identifier of the data is going to be processed.
            output_queue (mp.queues.Queue): to save the identifier and the corresponding result from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes

        # Run
        if self.identifier:
            # Run
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_with_indexes_all_data,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'data': self.input_data,
                        'data_len': self.data_len,
                        'shared_value': shared_value,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        else:
            # Run
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_without_indexes_all_data,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'data': self.input_data,
                        'data_len': self.data_len,
                        'shared_value': shared_value,
                        'output_queue': output_queue,
                    },
                )
                p.start()
                processes[i] = p
        for p in processes: p.join()

    def _multiprocess_while(self, shared_value: mp.managers.ValueProxy, output_queue: mp.queues.Queue) -> None:
        """
        Multiprocessing all the data using a while loop. This means that the function to be multiprocessed should only take 
        one index of the main data as the first function argument. 

        Args:
            input_queue (mp.queues.Queue): an empty queue to be populated.
            output_queue (mp.queues.Queue): to save the identifier and the corresponding result from the multiprocessing.
        """

        # Initial setup
        processes = [None] * self.nb_processes

        # Run
        if self.identifier:
            print('here is done also', flush=True)
            for i in range(self.nb_processes):
                p = mp.Process(
                    target=self._multiprocessing_while_sub_with_indexes,
                    kwargs={
                        'input_function': self.function,
                        'function_kwargs': self.function_kwargs,
                        'data': self.input_data,
                        'data_len': self.data_len,
                        'shared_value': shared_value,
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
                        'data': self.input_data,
                        'data_len': self.data_len,
                        'shared_value': shared_value,
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
            data: dict[str, any] | np.ndarray,
            data_len: int,
            shared_value: mp.managers.ValueProxy,
            output_queue: mp.queues.Queue,
        ) -> None:
        """ #TODO: change docstring
        To run a process in a while loop given an input and output queue. The data is accessed through the information of a shared memory object.
        Furthermore, the identifier is also given to the function to be multiprocessed as the second argument of the function.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            data (dict[str, any]): the shared memory information to point to the data.
            input_queue (mp.queues.Queue): to get the identifier to know what part of the data is being processed.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """

        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)
        
        # Run
        while True:
            value = shared_value.value
            if value >= data_len: break
            shared_value.value += 1

            # Get result
            result = input_function(data, value, **function_kwargs)
            # Save result
            output_queue.put((value, result))    

        if shm is not None: shm.close()

    @staticmethod
    def _multiprocessing_while_sub_without_indexes_all_data(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            data: dict[str, any] | np.ndarray,
            data_len: int,
            shared_value: mp.managers.ValueProxy,
            output_queue: mp.queues.Queue,
        ) -> None:
        """ #TODO: change the docstring.
        To run a process in a while loop given an input and output queue. The data is accessed through the information of a shared memory object.

        Args:
            input_function (typing.Callable[..., any]): the function that needs multiprocessing.
            function_kwargs (dict[str, any]): the keyword arguments for the function to be multiprocessed.
            data (dict[str, any]): the shared memory information to point to the data.
            output_queue (mp.queues.Queue): to save the identifier and the process result.
        """

        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)
        
        # Run
        while True:
            value = shared_value.value
            if value >= data_len: break
            shared_value.value += 1

            # Get result
            result = input_function(data, **function_kwargs)
            # Save result
            output_queue.put((value, result))

        if shm is not None: shm.close()

    @staticmethod
    def _multiprocessing_while_sub_with_indexes(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            data: dict[str, any] | np.ndarray,
            data_len: int,
            shared_value: mp.managers.ValueProxy,
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

        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)
        
        # Run
        while True:
            print('once', flush=True)
            value = shared_value.value
            if value >= data_len: break
            shared_value.value += 1

            # Get result
            result = input_function(data[value], value, **function_kwargs)
            # Save result
            output_queue.put((value, result))       

        if shm is not None: shm.close()

    @staticmethod
    def _multiprocessing_while_sub_without_indexes(
            input_function: typing.Callable[..., any],
            function_kwargs: dict[str, any],
            data: dict[str, any] | np.ndarray,
            data_len: int,
            shared_value: mp.managers.ValueProxy,
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

        shm = None
        if isinstance(data, dict) and ('name' in data.keys()): shm, data = MultiProcessing.open_shared_memory(data)
        
        # Run
        while True:
            value = shared_value.value
            if value >= data_len: break
            shared_value.value += 1

            # Get result
            result = input_function(data[value], **function_kwargs)
            # Save result
            output_queue.put((value, result))           

        if shm is not None: shm.close()     

    @staticmethod
    def shared_memory_multiple(data: np.ndarray | list[np.ndarray]) -> tuple[SharedMemoryList, dict[str, list]]:
        """
        To create multiple multiprocessing shared memory objects and return the corresponding references and information
        to then be able to re-access or unlink the memories.

        Args:
            data (np.ndarray | list[np.ndarray]): the data to be stored in shared memory objects.

        Returns:
            tuple[SharedMemoryList, dict[str, list]]: the shared memory pointers saved as an instance of the SharedMemoryList class so that
                .unlink() and .close() work directly on the references. The other output in the tuple is a dictionary containing the necessary 
                information to re-access the shared memory objects.
        """

        shm = [
            mp.shared_memory.SharedMemory(create=True, size=section.nbytes)
            for section in data
        ]
        info = {
            'name': [memory.name for memory in shm],
            'shape': [section.shape for section in data],
            'dtype': [section.dtype for section in data],
        }
        for i, section in enumerate(data):
            shared_array = np.ndarray(info['shape'][i], dtype=info['dtype'][i], buffer=shm[i].buf)
            np.copyto(shared_array, section)
            shm[i].close()
        return SharedMemoryList(shm), info

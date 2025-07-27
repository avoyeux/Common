"""
Custom manager.
"""
from __future__ import annotations

# IMPORTs standard
import time
import threading

# IMPORTs sub
from multiprocessing import Lock
from multiprocessing.managers import BaseManager

# IMPORTs local
from .manager_dataclass import TaskResult, TaskIdentifier, AllResults, FetchInfo

# TYPE ANNOTATIONs
from typing import (
    Any, Protocol, cast, Iterable, Callable, Generator, overload, Literal, TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..task_allocator import ProcessCoordinator  # ! gives a circular import
    from multiprocessing.synchronize import Lock as mp_lock
type TaskValue = tuple[FetchInfo, ProcessCoordinator| None, bool]

# API public
__all__ = ['CustomManager', 'TaskValue']



class CustomManagerProtocol(Protocol):
    """
    Protocol defining the interface of my CustomManager.
    """

    def Integer(self) -> Integer:
        """
        Create an shared integer that can be used to store and manipulate an integer value.
        'plus' to add 1 and 'minus' to subtract 1. Starting value is 0.
        """
        ...

    def Stack(self) -> Stack:
        """
        todo update docstring
        A list used as a stack to store the tasks to submit and generate the submission when the
        processors ask for them.
        No Locks. Implement them outside.
        """
        ...

    def StackTracker(self) -> StackTracker:
        # todo add docstring
        ...

    def Results(self) -> Results:
        """
        To handle the adding and getting results from tasks.
        put() method to add results and results() method to get the sorted results from the same
        set of tasks. Hence, the results method is blocking until that set of results is complete
        while the put method waits for a lock to add the result to a sorting queue.
        """
        ...

    def Lock(self) -> mp_lock:
        """
        Returns a proxy to a non-recursive lock object
        """
        ...

    def start(self, initializer=None, initargs: Iterable[Any] =()) -> None:
        """
        Spawns a server process for this manager object.
        """
        ...

    def shutdown(self) -> None:
        """
        To shutdown the manager and its server process.
        """
        ...


class Integer:
    """
    todo update docstring and most likely add a lock
    A super simple class to save an integer in the manager.
    """

    def __init__(self) -> None:
        """
        Create two instances of 0.
        One to store the number of tasks groups finished.
        One to get a unique identifier for each task group.
        """

        # DATA
        self._value = 0
        self._lock = threading.Lock()

    # def get(self) -> int:
    #     """
    #     Get the integer value.

    #     Returns:
    #         int: the integer value.
    #     """

    #     return self.value

    def plus(self, number: int = 1) -> int:
        """
        todo update docstring
        Increment the integer value by 1.
        This method is thread-safe.
        """

        self._lock.acquire()
        self._value += number
        value = self._value
        self._lock.release()
        return value
    
    def minus(self, number: int = 1) -> None:
        """
        todo update docstring
        Decrement the integer value by 1.
        This method is thread-safe.
        """

        self._lock.acquire()
        self._value -= number
        self._lock.release()


class StackTracker:
    # todo add docstring

    def __init__(self) -> None:
        # todo add docstring

        # LOCK n DATA
        self._lock = threading.Lock()
        self._list: list[tuple[int, Generator[bool | None, None, None]]] = []

    # def __next__(self) -> int:
    #     # todo add docstring
    #     # * this should be able to keep track of the tasks left in each queue even if there is no
    #     # * lock between this and the worker get calls.

    #     self._lock.acquire()
    #     while True:
    #         value, generator = self._list[-1]
    #         next_value = next(generator)

    #         if next_value: break
    #         self._list.pop()  # remove finished generator
    #     self._lock.release()
    #     return value

    def next(self) -> int:
        # todo add docstring
        # * this should be able to keep track of the tasks left in each queue even if there is no
        # * lock between this and the worker get calls.

        self._lock.acquire()
        while True:
            value, generator = self._list[-1]
            exists = next(generator)

            if exists: break
            self._list.pop()  # remove finished generator
        self._lock.release()
        return value

    def add(self, stack_index: int, number_of_tasks: int) -> None:
        # todo add docstring

        self._lock.acquire()
        self._list.append((stack_index, self._generator(number_of_tasks)))
        self._lock.release()

    def _generator(self, number_of_tasks: int) -> Generator[bool | None, None, None]:
        # todo add docstring

        for i in range(number_of_tasks): yield True
        yield False  # done


class Stack:
    """
    A list used as a stack to store the tasks to submit and generate the submission when the
    workers ask for them.
    Also keeps track of the number of tasks in the stack and create the unique identifier for each
    task group.
    No Locks. Implement them outside.
    """

    def __init__(self) -> None:
        """
        todo update docstring
        Initialize an empty list to store TaskValue items.
        The list is used as a stack to store the tasks to submit and generate the submission when
        the workers ask for them.
        Also keeps track of the number of tasks in the stack and create the unique identifier for
        each task group.
        No Locks. Implement them outside.
        """

        # STACK init
        self._list: list[
            tuple[
                Generator[int | None, None, None],
                int,
                int,
                Callable[..., Any],
                Generator[dict[str, Any], None, None],
                ProcessCoordinator | None,
                bool,
            ]
        ] = []

        # METADATA
        self._count: int = 0

        # LOCK
        self._lock = threading.Lock()

    @overload
    def put(
            self,
            unique_id: int,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[True] = ...,
            function_kwargs:
                dict[str, Any] |
                tuple[
                    Callable[..., Generator[dict[str, Any], None, None]],
                    tuple[Any, ...],
                ] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier: ...

    @overload
    def put(
            self,
            unique_id: int,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[False],
            function_kwargs:
                dict[str, Any] |
                tuple[
                    Callable[..., Generator[dict[str, Any], None, None]],
                    tuple[Any, ...],
                ] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> None: ...

    # FALLBACK
    @overload
    def put(
            self,
            unique_id: int,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = ...,
            function_kwargs:
                dict[str, Any] |
                tuple[
                    Callable[..., Generator[dict[str, Any], None, None]],
                    tuple[Any, ...],
                ] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier | None: ...

    def put(
            self,
            unique_id: int,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = True,
            function_kwargs:
                dict[str, Any] |
                tuple[
                    Callable[..., Generator[dict[str, Any], None, None]],
                    tuple[Any, ...],
                ] = {},
            coordinator: ProcessCoordinator | None = None,
        ) -> TaskIdentifier | None:
        """
        todo update docstring
        Submits a group of tasks to the input stack.
        If you are planning to also submit tasks inside this call, then you should pass the
        'ProcessCoordinator' instance to the function. Make sure that 'function' has a
        'coordinator' keyword argument that will be set to the ProcessCoordinator instance.

        Args:
            number_of_tasks (int): the number of tasks to submit.
            function (Callable[..., Any]): the function to run for each task.
            function_kwargs (
                dict[str, Any] |
                tuple[
                    Callable[..., Generator[dict[str, Any], None, None]],
                    tuple[Any, ...],
                ],
            optional): the keyword arguments for the function. If the arguments have to be
                different for each tasks, then a tuple containing a generator callable and the args
                for the generator (as a tuple too) should be passed. The generator should yield
                the keyword arguments for each task. Keep in mind that the generator callable
                should be picklable, i.e. must be defined as a function or as a top level class
                static method. Defaults to an empty dict.
            coordinator (ProcessCoordinator | None, optional): the coordinator instance to
                associate with the tasks. Used if you want to do some nested multiprocessing.
                Defaults to None.

        Returns:
            TaskIdentifier: the identifier of the task(s) that were just added to the stack.
        """

        if isinstance(function_kwargs, dict):
            generator = self._input_generator(number_of_tasks, function_kwargs)
        else:
            gen, args = function_kwargs
            generator = gen(*args)

        # INDEX generator
        index_generator = self._index_generator(number_of_tasks)
        
        # ADD tasks to waiting list
        self._lock.acquire()
        self._count += number_of_tasks
        self._list.append((
            index_generator,
            number_of_tasks,
            unique_id,
            function,
            generator,
            coordinator,
            results,
        ))
        if not results: self._lock.release(); return  # no results expected
        # IDENTIFIER to find results
        identifier = TaskIdentifier(
            index=0,
            process_id=unique_id,
            number_tasks=number_of_tasks,
        )
        self._lock.release()
        return identifier

    def get(self) -> TaskValue:
        """
        todo explain that empty() needs to be called before to have the right count
        To get a task from the input stack.
        The information of the stack are gotten from generators to save RAM and keep track.

        Returns:
            TaskValue: the corresponding task to be run by the workers.
        """

        self._lock.acquire()
        while True:
            index_generator, nb, p_id, function, kwargs_generator, coordinator, results = (
                self._list[-1]
            )

            # CHECK generator
            index = next(index_generator)
            if index is not None: break

            # POP finished tasks
            self._list.pop()
        
        # GET kwargs
        kwargs = next(kwargs_generator)
        self._lock.release()

        # FORMAT FetchInfo
        fetch_info = FetchInfo(
            identifier=TaskIdentifier(
                index=index,
                process_id=p_id,
                number_tasks=nb,
            ),
            function=function,
            kwargs=kwargs,
        )
        return fetch_info, coordinator, results

    def empty(self) -> bool:
        """
        # ? is it deprecated because of the ManagerAllocator ?
        todo update docstring
        To check if there is no more tasks in the input stack.
        Cannot just check the results length as the last item is never popped.

        Returns:
            bool: True if no more tasks. 
        """

        self._lock.acquire()
        check = (self._count == 0)
        if not check: self._count -= 1  # promise to use a task
        self._lock.release()
        return check
    
    def _index_generator(
            self,
            number_of_tasks: int,
        ) -> Generator[int | None, None, None]:
        """
        Generator to yield the index of the task in the input stack.

        Args:
            number_of_tasks (int): the number of tasks to generate indices for.

        Yields:
            Generator[int | None, None, None]: the generator yielding the indices of the tasks.
                Yields None when all indices have been generated.
        """

        for i in range(number_of_tasks): yield i
        yield None  # done

    def _input_generator(
            self,
            number_of_tasks: int,
            function_kwargs: dict[str, Any],
        ) -> Generator[dict[str, Any], None, None]:
        """
        Generator to yield the keyword arguments for each task.
        Only used if the arguments are the same for each task.

        Args:
            number_of_tasks (int): the number of tasks to generate keyword arguments for.
            function_kwargs (dict[str, Any]): the function kwargs to yield.

        Yields:
            Generator[dict[str, Any], None, None]: the generator yielding the keyword arguments for
                each task.
        """

        for _ in range(number_of_tasks): yield function_kwargs


class Results:
    """
    todo update docstring
    To handle the adding and getting results from tasks.
    put() method to add results and results() method to get the sorted results from the same set of
    tasks. Hence, the results method is blocking until that set of results is complete while the
    put method waits for a lock to add the result to a sorting queue.
    """

    def __init__(self) -> None:
        """
        Create an instance that handles the adding and getting of the results from the tasks.
        Two threads will run:
            - one to add the results to the results queue.
            - one to check the results queue and add the results to the AllResults instance.
        """

        # LOCK n DATA
        self._lock = threading.Lock()
        self._results_queue: list[TaskResult] = []
        self._all_results: AllResults = AllResults()

        # RESULTs check and formatting
        results_check_thread = threading.Thread(target=self._format_result, daemon=True)
        results_check_thread.start()
    
    def put(self, task_identifier: TaskIdentifier, data: Any) -> None:
        """
        To add a result to the results queue.

        Args:
            task_identifier (TaskIdentifier): the identifier unique to each task sent to the
                manager.
            data (Any): the data to add to the results queue.
        """

        result = TaskResult(identifier=task_identifier, data=data)
        with self._lock: self._results_queue.append(result)

    def give(self, identifier: TaskIdentifier) -> list[Any]:
        """
        To get the results the same group of tasks.
        Keep in mind that you should first check results_full() to ensure that the results are
        ready.

        Args:
            identifier (TaskIdentifier): the identifier unique to each task sent to the manager.

        Returns:
            list[Any]: ordered data list of the results of a given group of tasks.
        """

        # WAIT n GET results
        same_results = self._all_results.get(identifier)
        return [task.data for task in same_results.data]
    
    def full(self, identifier: TaskIdentifier) -> bool:
        """
        To check if the results for a given task identifier are ready.

        Args:
            identifier (TaskIdentifier): the identifier unique to each task sent to the manager.

        Returns:
            bool: True if the results are ready, False otherwise.
        """

        # CHECK if results are ready
        return self._all_results.results_full(identifier)

    def _format_result(self) -> None:
        """
        Formats the results by popping them from the queue and adding them to _all_results.
        This method runs in a separate thread to continuously check for new results.
        """

        while True:
            result = self._get_result()
            self._all_results.add(result)

    def _get_result(self) -> TaskResult:
        """
        Gets a result from the results queue, blocking until a result is available.

        Returns:
            SingleResult: the result from the queue.
        """

        while True:
            result = self._pop_result()
            if result is not None: return result
            time.sleep(0.5)  # todo add it as a parameter for the class
            
    def _pop_result(self) -> TaskResult | None:
        """
        Pops a result from the results list.

        Returns:
            SingleResult | None: The popped result or None if the list is empty.
        """

        with self._lock:
            if self._results_queue: return self._results_queue.pop(0)
            else: return None


# MANAGER creation
class CustomManager(BaseManager):
    """
    Custom manager that contains a stack (List), an integer (Integer), a results handler (Results),
    and a lock (Lock). This manager is used to handle the multiprocessing tasks in the
    ProcessCoordinator.
    To use it, don't forget to call the start() method.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> CustomManagerProtocol:
        """
        Just to ensure that type checkers recognise the manager's internal structure.

        Returns:
            CustomManagerProtocol: the manager instance with the correct protocol.
        """

        return cast(CustomManagerProtocol, super().__new__(cls, *args, **kwargs))


# MANAGER registration
CustomManager.register("Lock", Lock)
CustomManager.register("Stack", Stack)
CustomManager.register("Integer", Integer)
CustomManager.register("Results", Results)
CustomManager.register("StackTracker", StackTracker)



# TESTING
if __name__ == "__main__":

    manager = CustomManager()
    manager.start()
    results = manager.Results()
    manager.shutdown()

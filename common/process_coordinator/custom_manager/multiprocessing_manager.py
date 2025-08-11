"""
Custom manager.
"""
from __future__ import annotations

# IMPORTs standard
import time
import threading

# IMPORTs sub
from multiprocessing.managers import BaseManager

# IMPORTs local
from ..utils import IndexAllocator
from .manager_dataclass import TaskResult, TaskIdentifier, AllResults, FetchInfo

# TYPE ANNOTATIONs
from typing import Any, Protocol, cast, Iterable, Callable, Generator, overload, Literal
type TaskValue = tuple[FetchInfo, bool]

# API public
__all__ = ['CustomManager', 'TaskValue']



class CustomManagerProtocol(Protocol):
    """
    Protocol defining the interface of my CustomManager.
    """

    def Stack(self) -> Stack:
        """
        Proxy stack to store the tasks to submit and sends them to the workers when asked for it.
        """
        ...

    def Results(self) -> Results:
        """
        Proxy to handle the adding and getting results from tasks.
        'put' method to add results and 'give' method to get the sorted results from the same set
        of tasks. Keep in mind that the 'give' method doesn't wait for the results to be ready and
        as such you should first check the return of the 'full' method to ensure that the results
        are ready.
        A lock is implemented internally.
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


class Stack:
    """
    A list used as a stack to store the tasks to submit and generate the submission when the
    workers ask for them.
    Lock implemented internally.
    """

    def __init__(self) -> None:
        """
        Initialize an empty list to store TaskValue items.
        The list is used as a stack to store the tasks to submit and generate the submission when
        the workers ask for them.

        Lock implemented internally.
        """

        # STACK init
        self._list: list[
            tuple[
                Generator[int | None, None, None],
                int,
                int,
                int,
                Callable[..., Any],
                Generator[dict[str, Any], None, None],
                bool,
            ]
        ] = []

    def put(
            self,
            group_id: int,
            total_tasks: int,
            first_task_index: int,
            last_task_index: int,
            function: Callable[..., Any],
            results: bool = True,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            length_difference: bool = False,
        ) -> None:
        """
        Submits a group of tasks to the input stack.

        Args:
            group_id (int): the unique ID for the group of tasks.
            total_tasks (int): the total number of tasks in the group. This is not the same thing
                that the number of group tasks inside this queue, but represents the total number
                of tasks for that same group across all queues.
            first_task_index (int): the index of the first task in the group for this queue.
            last_task_index (int): the index of the last task in the group for this queue.
            function (Callable[..., Any]): the function to run for each task.
            results (bool, optional): whether to return the results of the tasks. Defaults to True.
            same_kwargs (dict[str, Any], optional): the keyword arguments that are the same for
                all tasks in this group. Defaults to {}.
            different_kwargs (dict[str, list[Any]], optional): the keyword arguments that are
                different for each task in this group. If 'length_difference' is False, the tasks
                will each take one element of the list for each key. If it is True, then each task
                will get a slice of items from each list. Defaults to {}.
            length_difference (bool, optional): whether the lists in 'different_kwargs' for all the
                queues have different lengths than the total number of tasks. This is to track if
                each task should get a slice of items from each list or just a unique item.
                Defaults to False.
        """
        nb_of_tasks = last_task_index - first_task_index + 1
        # KWARGS generator
        generator = self._input_generator(
            number_of_tasks=nb_of_tasks, 
            same_kwargs=same_kwargs,
            different_kwargs=different_kwargs,
            length_difference=length_difference,
        )


        # INDEX generator
        index_generator = self._index_generator(
            first_index=first_task_index,
            last_index=last_task_index,
        )

        # ADD tasks to waiting list
        self._list.append((
            index_generator,
            nb_of_tasks,
            total_tasks,
            group_id,
            function,
            generator,
            results,
        ))

    def get(self) -> TaskValue:
        """
        To get a task from the input stack.
        The information of the stack are gotten from generators to save RAM and keep track.
        This method will only work if the stack is not empty, hence you first need to run a code
        making sure that the stack itself is not empty.

        Returns:
            TaskValue: the corresponding task to be run by the workers.
        """

        while True:
            (
                index_generator, group_tasks, total_tasks, group_id, function, kwargs_generator,
                results,
            ) = self._list[-1]
            # CHECK generator
            index = next(index_generator)
            if index is not None: break

            # POP finished tasks
            self._list.pop()
        
        # GET kwargs
        kwargs = next(kwargs_generator)

        # FORMAT FetchInfo
        fetch_info = FetchInfo(
            identifier=TaskIdentifier(
                index=index,
                group_id=group_id,
                total_tasks=total_tasks,
                group_tasks=group_tasks,
            ),
            function=function,
            kwargs=kwargs,
        )
        return fetch_info, results
    
    def _index_generator(
            self,
            first_index: int,
            last_index: int,
        ) -> Generator[int | None, None, None]:
        """
        Generator to yield the index of the task in the input stack.

        Args:
            first_index (int): the index of the first task in the group for this queue.
            last_index (int): the index of the last task in the group for this queue.

        Yields:
            Generator[int | None, None, None]: the generator yielding the indices of the tasks.
                Yields None when all indices have been generated.
        """

        for i in range(first_index, last_index + 1): yield i
        yield None  # done

    @overload
    def _input_generator(
            self,
            number_of_tasks: int,
            same_kwargs: dict[str, Any],
            different_kwargs: dict[str, list[Any]],
            length_difference: Literal[False],
        ) -> Generator[dict[str, Any], None, None]: ...
    
    @overload
    def _input_generator(
            self,
            number_of_tasks: int,
            same_kwargs: dict[str, Any],
            different_kwargs: dict[str, list[Any]],
            length_difference: Literal[True],
        ) -> Generator[dict[str, list[Any]], None, None]: ...
    
    # FALLBACK
    @overload
    def _input_generator(
            self,
            number_of_tasks: int,
            same_kwargs: dict[str, Any],
            different_kwargs: dict[str, list[Any]],
            length_difference: bool,
        ) -> Generator[dict[str, Any] | dict[str, list[Any]], None, None]: ...

    def _input_generator(
            self,
            number_of_tasks: int,
            same_kwargs: dict[str, Any],
            different_kwargs: dict[str, list[Any]],
            length_difference: bool,
        ) -> Generator[dict[str, Any] | dict[str, list[Any]], None, None]:
        """
        Generator to yield the keyword arguments for each task.

        Args:
            number_of_tasks (int): the number of tasks to generate keyword arguments for.
            same_kwargs (dict[str, Any]): the keyword arguments that are the same for all tasks in
                this group.
            different_kwargs (dict[str, list[Any]]): the keyword arguments that are different for
                each task in this group. If not 'length_difference', the generator will yield one
                unique item of each list for each key. If True, then the lists are partitioned
                across the tasks. Hence, each tasks will still get a list of items for each key
                (but slices of the initial lists).
            length_difference (bool): whether the lists in different_kwargs for all the queues have
                different lengths than the total number of tasks. This is to track if each task
                should get a slice of items from each list or just a unique item.

        Yields:
            Generator[dict[str, Any], None, None]: the generator yielding the keyword arguments for
                each task.
        """

        # CHECK args length
        if length_difference:
            list_length = len(next(iter(different_kwargs.values())))
            args_per_task = IndexAllocator.valid_indexes(
                number_of_tasks=list_length,
                nb_of_queues=number_of_tasks,
            )

            for i in range(number_of_tasks):
                first_index = sum(args_per_task[:i])
                last_index = first_index + args_per_task[i] - 1
                different = {
                    k: v[first_index: last_index + 1] for k, v in different_kwargs.items()
                }
                yield {**same_kwargs, **different}
        else:
            # SAME number of args and tasks
            for i in range(number_of_tasks):
                different = {k: v[i] for k, v in different_kwargs.items()}
                yield {**same_kwargs, **different}


class Results:
    """
    To handle the adding and getting results from tasks.
    'put' method to add results and 'give' method to get the sorted results from the same set of
    tasks. Keep in mind that the 'give' method doesn't wait for the results to be ready and as
    such you should first check the return of the 'full' method to ensure that the results are
    ready.
    """

    def __init__(self) -> None:
        """
        Create an instance that handles the adding and getting of the results from the tasks.
        Two threads will run:
            - one to add the results to the results queue.
            - one to check the results queue and add the results to the AllResults instance.
        
        The public methods are:
            - 'put' method to add results 
            - 'give' method to get the sorted results from the same set of tasks (doesn't wait for
                the results to be ready).
            - 'full' method to check if the results for a given task identifier are ready. Should
                be used before calling the 'give' method to ensure that the results are ready. 
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

    def give(self, identifier: TaskIdentifier) -> list[tuple[int, Any]]:
        """
        To get the results the same group of tasks.
        Keep in mind that you should first check the 'full' method to ensure that the results are
        ready.

        Args:
            identifier (TaskIdentifier): the identifier unique to each task sent to the manager.

        Returns:
            list[Any]: ordered data list of the results of a given group of tasks.
        """

        # GET results
        same_results = self._all_results.get(identifier)
        return [(task.identifier.index, task.data) for task in same_results.data]
    
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
            time.sleep(0.5)
            
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
    Custom manager that contains an integer (Integer), a queue index tracker (IndexTracker),
    a stack (Stack), and a results handler (Results). This manager is used to handle the
    multiprocessing tasks in the ProcessCoordinator using the ManagerAllocator class.
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
CustomManager.register("Stack", Stack)
CustomManager.register("Results", Results)

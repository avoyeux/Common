"""
Custom manager.
"""
from __future__ import annotations

# IMPORTs standard
import time
import queue
import threading

# IMPORTs sub
from multiprocessing.managers import BaseManager

# IMPORTs local
from .manager_dataclass import TaskResult, TaskIdentifier, AllResults, FetchInfo

# TYPE ANNOTATIONs
from typing import Any, Protocol, cast, Iterable, TYPE_CHECKING
if TYPE_CHECKING: from ..task_allocator import ProcessCoordinator  # ! gives a circular import
type CustomQueue = queue.Queue[tuple[FetchInfo, ProcessCoordinator| None] | None]

# API public
__all__ = ['CustomManager', 'CustomQueue']



class CustomManagerProtocol(Protocol):
    """
    Protocol defining the interface of my CustomManager.
    """

    def Results(self) -> Results:
        """
        To handle the adding and getting results from tasks.
        put() method to add results and results() method to get the sorted results from the same
        set of tasks. Hence, the results method is blocking until that set of results is complete
        while the put method waits for a lock to add the result to a sorting queue.
        """
        ...

    def Queue(self, maxsize: int = 0) -> CustomQueue:
        """
        Create a queue object with a given maximum size.
        If maxsize is <= 0, the queue size is infinite.
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


class Results:
    """
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
        """

        result = TaskResult(identifier=task_identifier, data=data)
        with self._lock: self._results_queue.append(result)

    def results(self, identifier: TaskIdentifier) -> list[Any]:
        """
        To get the wait for and get the results of a group of tasks.
        Basically used the same way than when one uses the .join() method when multiprocessing.

        Args:
            identifier (TaskIdentifier): the identifier unique to each task sent to the manager.

        Returns:
            list[Any]: ordered data list of the results of a given group of tasks.
        """

        # WAIT n GET results
        same_results = self._all_results.get(identifier)
        return [task.data for task in same_results.data]

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
    Custom manager created to be able to register the Results class.
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
CustomManager.register("Results", Results)
CustomManager.register('Queue', queue.Queue)



# TESTING
if __name__ == "__main__":

    manager = CustomManager()
    manager.start()
    results = manager.Results()
    manager.shutdown()

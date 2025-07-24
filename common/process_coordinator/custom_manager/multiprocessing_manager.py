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
from typing import Any, Protocol, cast, Iterable, TYPE_CHECKING
if TYPE_CHECKING:
    from ..task_allocator import ProcessCoordinator  # ! gives a circular import
    from multiprocessing.synchronize import Lock as mp_lock
type TaskValue = tuple[FetchInfo, ProcessCoordinator| None]

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

    def List(self) -> List:
        """
        Simple empty list used as a stack to store TaskValue items.
        No Locks or list length checks before the .pop(). Implement them outside.
        """
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
    A super simple class to save an integer in the manager.
    """

    def __init__(self) -> None:
        """
        Create two instances of 0.
        One to store the number of tasks groups finished.
        One to get a unique identifier for each task group.
        """

        # LOCK n DATA
        self.value = 0
        self.count = -1
        self._lock_count = threading.Lock()

    def get(self) -> int:
        """
        Get the integer value.

        Returns:
            int: the integer value.
        """

        return self.value

    def plus(self) -> None:
        """
        Increment the integer value by 1.
        This method is thread-safe.
        """

        self.value += 1
    
    def minus(self) -> None:
        """
        Decrement the integer value by 1.
        This method is thread-safe.
        """

        self.value -= 1

    def next(self) -> int:
        """
        Get the next unique identifier for a task group.

        Returns:
            int: the next unique identifier for a task group.
        """

        with self._lock_count: self.count += 1
        return self.count


class List:
    """
    Simple empty list used as a stack to store TaskValue items.
    No Locks or list length checks before the .pop(). Implement them outside.
    """

    def __init__(self) -> None:
        """
        Initialize an empty list to store TaskValue items.
        Done to have a simple stack. No Locks or anything as the ProcessCoordinator does the
        locking and checking. (Have to to make sure there are no sudden stops).
        """

        self._list: list[TaskValue] = []

    
    def put(self, value: TaskValue) -> None:
        """
        Add a value to the list.

        Args:
            value (TaskValue): the value to add to the list.
        """

        self._list.append(value)
    
    def get(self) -> TaskValue:
        """
        Get a value from the list.

        Returns:
            TaskValue: the value from the list. raises an IndexError if the list is empty.
        """

        return self._list.pop()  # ! WRONG; but actual implementation does a check and lock


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
    
    def results_full(self, identifier: TaskIdentifier) -> bool:
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
CustomManager.register("List", List)
CustomManager.register("Integer", Integer)
CustomManager.register("Results", Results)



# TESTING
if __name__ == "__main__":

    manager = CustomManager()
    manager.start()
    results = manager.Results()
    manager.shutdown()

"""
Has the dataclasses needed for the data handling of the custom multiprocessing manager.
"""
from __future__ import annotations

# IMPORTs standard
import time

# IMPORTs sub
from dataclasses import dataclass, field

# TYPE ANNOTATIONs
from typing import Any, cast, Self, Callable
from threading import Lock

# API public
__all__ = ['FetchInfo', 'TaskIdentifier', 'TaskResult', 'AllResults']

# todo need to see if there is a better way to wait than use time sleep



@dataclass(slots=True, eq=False, repr=False, match_args=False)
class FetchInfo:
    """
    To store the information needed inside the custom manager queue.
    """

    identifier: TaskIdentifier
    function: Callable
    kwargs: dict[str, Any] = field(default_factory=dict)  # ? is this the right way to do it?

    def __getstate__(self) -> dict[str, Any]:
        """
        To serialize the instance attributes.
        As slots=True, then self.__dict__ doesn't exist, hence the getattr approach.

        Returns:
            dict[str, Any]: contains all the instance attributes.
        """

        return {slot: getattr(self, slot) for slot in self.__slots__}

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        To deserialize the instance attributes.
        As slots=True, then self.__dict__ doesn't exist, hence the setattr approach.

        Args:
            state (dict[str, Any]): contains all the instance attributes.
        """

        for slot, value in state.items(): setattr(self, slot, value)



@dataclass(slots=True, eq=False, repr=False, match_args=False)
class TaskIdentifier:
    """
    To identify to which task a result belongs.
    """

    index: int
    number_tasks: int
    process_id: int

    def __getstate__(self) -> dict[str, int]:
        """
        To get the state of the TaskIdentifier (i.e. serialize it).
        The class uses slots, hence the non-standard getstate dunder method.

        Returns:
            dict[str, int]: the state of the TaskIdentifier.
        """
        
        return {slot: getattr(self, slot) for slot in self.__slots__}
    
    def __setstate__(self, state: dict[str, int]) -> None:
        """
        To set the state of the TaskIdentifier (i.e. deserialize it).
        The class uses slots, hence the non-standard setstate dunder method.

        Args:
            state (dict[str, int]): the state to set.
        """

        for slot, value in state.items(): setattr(self, slot, value)


@dataclass(slots=True, eq=False, repr=False, match_args=False)
class TaskResult:
    """
    To store a single task result gotten from a worker process.
    """

    identifier: TaskIdentifier
    data: Any


@dataclass(slots=True, eq=False, repr=False, match_args=False)
class SameIdentifier:
    """
    To identify a results gotten for the same task.
    Used to regroup results together and filter the results (for the join method).
    """

    # ! make sure that the first task number is 0
    # ? should I add a task_failed attribute ?

    full: bool = False  # if all the tasks are done
    process_id: int | None = None
    number_tasks: int | None = None
    tasks_done: list[int] = field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        """
        Checks if the other object is compatible with this SameIdentifier.

        Args:
            other (object): the object to compare with.

        Returns:
            bool: returns True if the other object is a SameIdentifier and is compatible with
                this one.
        """

        if isinstance(other, (TaskIdentifier, SameIdentifier)):
            return all([
                self.process_id == other.process_id,
                self.number_tasks == other.number_tasks,
            ])
        else:
            raise NotImplementedError(
                f"Cannot compare SameIdentifier with {type(other)}."
            )

    def add(self, identifier: TaskIdentifier) -> None: 
        """
        Adds a TaskIdentifier to the SameIdentifier.
        'check_add' should be called before to make sure the identifier can be added. Hence, while
        this is not a private method, it should not be used outside of this file as the user might
        forget to call 'check_add' first (the add still adds even if it shouldn't).

        Args:
            identifier (TaskIdentifier): the identifier of the task to add.
        """

        self.tasks_done.append(identifier.index)

        # CHECK empty
        if self.process_id is None:
            self.process_id = identifier.process_id
            self.number_tasks = identifier.number_tasks
        
        # UPDATE full
        self._update_full()
    
    def check_add(self, identifier: TaskIdentifier) -> bool:
        """
        Checks if the identifier can be added to the ResultIdentifier.

        Args:
            identifier (TaskIdentifier): the identifier of the task to check.

        Returns:
            bool: True if the identifier can be added, False otherwise.
        """

        if all([
            self.process_id is None or self.process_id == identifier.process_id,
            self.number_tasks is None or self.number_tasks == identifier.number_tasks,
            identifier.index not in self.tasks_done,
        ]):
            return True
        return False
    
    def _update_full(self) -> None:
        """
        Updates the 'full' attribute of the SameIdentifier based on the tasks_done list.
        """

        # * I could just do a len() check but keeping this for now to make sure the code is doing
        # * the right thing
        if set(self.tasks_done) == set(range(cast(int, self.number_tasks))): self.full = True


@dataclass(slots=True, eq=False, repr=False, match_args=False)
class SameResults:
    """
    To store the results that would define the same task.
    Used to decide what information to give when calling the .join() method.
    """

    identifier: SameIdentifier = field(default_factory=SameIdentifier)
    data: list[TaskResult] = field(default_factory=list)

    def add(self, result: TaskResult) -> None:
        """
        Adds a TaskResult to the SameResults if it can be added.

        Args:
            result (TaskResult): the result to add.
        """

        self.identifier.add(result.identifier)
        self.data.append(result)
    
    def sorted(self) -> Self:
        """
        Sorts the data list by index.
        That means that after sorting, the data is in the same order than the initial tasks.

        Returns:
            Self: the SameResults instance with the data list sorted.
        """

        self.identifier.tasks_done.sort()
        self.data.sort(key=lambda data: data.identifier.index)
        return self


@dataclass(slots=True, eq=False, repr=False, match_args=False)
class AllResults:
    """
    To store the results gotten from all worker processes.
    """

    _lock: Lock = Lock()
    data: list[SameResults] = field(default_factory=list)

    def get(self, identifier: TaskIdentifier) -> SameResults:
        """
        To get the full SameResults instance corresponding to the identifier.
        The code will block until the corresponding full SameResults is available.

        Args:
            identifier (TaskIdentifier): the task identifier to search the SameResults for.

        Returns:
            SameResults: the corresponding full SameResults instance.
        """

        # SEARCH loop
        while True:
            # FIND correct SameResults
            self._lock.acquire()
            for i, same_result in enumerate(self.data):
                # FOUND result
                if same_result.identifier == identifier:
                    # WAIT for full results
                    self._wait_for_full(same_result)

                    # RESULT removed
                    self.data.remove(same_result)
                    self._lock.release()
                    return same_result.sorted()

            # WAIT
            self._lock.release()
            time.sleep(1)  # ? add it as a parameter ?

    def _wait_for_full(self, same_results: SameResults) -> None:
        """
        Waits for the SameResults instance to be full.

        Args:
            same_results (SameResults): the result to wait for.
        """

        while True:
            if same_results.identifier.full: return
            
            # WAIT
            self._lock.release()
            time.sleep(1)  # ? add it as a parameter ?
            self._lock.acquire()

    def add(self, result: TaskResult) -> None:
        """
        Adds a TaskResult to the AllResults.
        To do so, the right SameResults is found or created.

        Args:
            result (TaskResult): the task result to add.
        """

        # LOCK to change data
        self._lock.acquire()

        # CHECK empty
        if not self.data:
            self.data.append(SameResults())
            self.data[0].add(result)

            # LOCK release
            self._lock.release()
            return
        
        # SAME RESULT find
        for same_result in self.data:
            if same_result.identifier.check_add(result.identifier):
                same_result.add(result)

                # LOCK release
                self._lock.release()
                return

        # NEW SAME RESULT create
        new_same_result = SameResults()
        new_same_result.add(result)
        self.data.append(new_same_result)
        self._lock.release()

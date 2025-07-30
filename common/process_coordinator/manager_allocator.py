"""
To allocate managers to the process coordinator.
This is needed if the size of the data to be sent between workers is large.
In this case multiple managers might be needed to serialize the data and not lead to the workers
not doing anything while waiting for the manager to serialize the data.
"""
from __future__ import annotations

# IMPORTs local
from .custom_manager import CustomManager, TaskIdentifier

# TYPE ANNOTATIONs
from typing import Any, Callable, overload, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from .custom_manager.multiprocessing_manager import (
        Results, Integer, Stack, CustomManagerProtocol, IndexTracker, TaskValue,
    )
    from .task_allocator import ProcessCoordinator

# API public
__all__ = ['ManagerAllocator']



class StartedStack:
    """
    Starts a custom manager with a corresponding stack.
    Hence, this class is used as a stack wrapper for the custom manager.
    """

    def __init__(self) -> None:
        """
        To start a manager and create a corresponding custom stack for storing and giving tasks.
        """

        manager = CustomManager()
        manager.start()
        self.manager = manager
        self.stack: Stack = self.manager.Stack()

    def __getstate__(self) -> dict[str, Stack]: return {"stack": self.stack}

    def __setstate__(self, state: dict[str, Stack]) -> None: self.__dict__.update(state)


class StartedSorter:
    """
    Starts a custom manager with a corresponding results object.
    Also contains a stack for when there is only one manager total.
    """

    def __init__(self) -> None:
        """
        To start a manager and create a corresponding results object for storing and giving
        results.
        It also creates a stack for when there is only one manager total.
        """

        manager = CustomManager()
        manager.start()
        self.manager = manager
        self.results: Results = self.manager.Results()
        self.stack: Stack = self.manager.Stack()  # for when there is only one manager total

    def __getstate__(self) -> dict[str, Any]:
        return {
            "results": self.results,
            "stack": self.stack,
        }

    def __setstate__(self, state: dict[str, Any]) -> None: self.__dict__.update(state)


class IndexAllocator:
    """
    Computes queue indexes and corresponding number of tasks for multiple managers.

    There are two main methods:
        - 'add': to add the number of tasks to the index tracker.
        - 'group_tasks': to compute the number of tasks for each queue based on the total number
            of tasks and the number of queues.
    """

    def __init__(self, manager: CustomManagerProtocol, manager_nb: tuple[int, int]) -> None:
        """
        To create an index allocator for the given manager and number of managers.
        Lets you compute the queue indexes and corresponding number of tasks for multiple managers.

        There are two main methods:
            - 'add': to add the number of tasks to the index tracker.
            - 'group_tasks': to compute the number of tasks for each queue based on the total
                number of tasks and the number of queues.

        Args:
            manager (CustomManagerProtocol): the manager used to create and share the index
                tracker.
            manager_nb (tuple[int, int]): the number of managers used in the ManagerAllocator.
        """

        # ATTRIBUTEs
        self._manager_nb = manager_nb
        self.stack: IndexTracker = manager.IndexTracker()

    def add(self, number_of_tasks: int) -> None:
        """
        To add a group of tasks to the index tracker.

        Args:
            number_of_tasks (int): the total number of tasks to add to the index tracker.
        """

        self.stack.add(number_of_tasks=number_of_tasks, number_of_queues=self._manager_nb[0])

    def group_tasks(self, number_of_tasks: int, nb_of_queues: int) -> list[int]:
        """
        To compute the number of tasks for each queue based on the total number of tasks and the
        number of queues.

        Args:
            number_of_tasks (int): the total number of tasks needed to be distributed.
            nb_of_queues (int): the number of queues to distribute the tasks across.

        Returns:
            list[int]: list of the number of tasks for each queue.
        """

        index_partition = self._partition_integer(total=number_of_tasks, n=nb_of_queues)
        return [n for n in index_partition if n > 0]

    def _partition_integer(self, total: int, n: int) -> list[int]:
        """
        Partition an integer into n parts as evenly as possible.
        This is done so that each submit partitions the tasks evenly across the managed stacks.

        Args:
            total (int): the total number to partition.
            n (int): the number of parts to partition into.

        Returns:
            list[int]: the partition sizes.
        """

        coef, res = divmod(total, n)
        return [coef + 1] * res + [coef] * (n - res)


class ManagerAllocator:
    """
    To allocate managers to the process coordinator.
    This is needed if the size of the data to be sent between workers is large.
    The public methods of this class are:
        - 'submit': to submit a group of tasks to the input stack.
        - 'get': to get a task from the input stack.
        - 'sort': to sort the results of a task.
        - 'give': to give the results of a group of tasks.
        - 'check': to check if there are tasks in the input stack and if there are results in
            waiting.
        - 'full': to check if the results stack is full for a given task identifier.
        - 'shutdown': to shutdown the managers.
    """

    def __init__(
            self,
            managers: int | tuple[int, int] = 1,
            verbose: int = 1,
            flush: bool = False,
        ) -> None:
        """
        Sets up the manager allocator with the given number of managers.
        The public methods of this class are:
            - 'submit': to submit a group of tasks to the input stack.
            - 'get': to get a task from the input stack.
            - 'sort': to sort the results of a task.
            - 'give': to give the results of a group of tasks.
            - 'check': to check if there are tasks in the input stack and if there are results in
                waiting.
            - 'full': to check if the results stack is full for a given task identifier.
            - 'shutdown': to shutdown the managers.

        Args:
            managers (int | tuple[int, int], optional): the number of managers to setup. When an
                integer bigger than one, one manager is created for the input stack and the rest
                are used for sorting the results. When a tuple, the first element is the number of
                managers for the input stack and the second element is the number of managers for
                sorting the results. Defaults to 1.
            verbose (int, optional): the verbosity level for the prints. Defaults to 1.
            flush (bool, optional): whether to flush the output after each print.
                Defaults to False.
        """

        # MANAGER arrangement
        self._manager_nb = self._manager_numbers(managers)

        # SHARED stack(s) and sorter(s)
        self._stacks: list[StartedStack] = [StartedStack() for _ in range(self._manager_nb[0])]
        self._sorters: list[StartedSorter] = [StartedSorter() for _ in range(self._manager_nb[1])]
        if not self._stacks: self._stacks = [self._sorters[0]]
        self._index_tracker: IndexAllocator = IndexAllocator(
            manager=self._stacks[0].manager,
            manager_nb=self._manager_nb,
        )

        # SHARED numbers
        self._unique_id: Integer = self._sorters[0].manager.Integer()
        self._stack_count: Integer = self._sorters[0].manager.Integer()
        self._sorter_count: Integer = self._sorters[0].manager.Integer()

        # ATTRIBUTEs settings
        self._verbose = verbose
        self._flush = flush

    def __getstate__(self) -> dict[str, Any]: return self.__dict__
    
    def __setstate__(self, state: dict[str, Any]) -> None: self.__dict__.update(state)

    @overload
    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[True],
            same_kwargs: dict[str, Any] = ...,
            different_kwargs: dict[str, list[Any]] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier: ...

    @overload
    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[False],
            same_kwargs: dict[str, Any] = ...,
            different_kwargs: dict[str, list[Any]] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> None: ...

    # FALLBACK
    @overload
    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = ...,
            same_kwargs: dict[str, Any] = ...,
            different_kwargs: dict[str, list[Any]] = ...,
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier | None: ...

    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = True,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            coordinator: ProcessCoordinator | None = None,
        ) -> TaskIdentifier | None:
        """
        Submits a group of tasks to the input stack(s).
        If you are planning to also submit tasks inside this call, then you should pass the
        'ProcessCoordinator' instance to the function. Make sure that 'function' has a
        'coordinator' keyword argument that will be set to the ProcessCoordinator instance.

        Args:
            number_of_tasks (int): the number of tasks to submit.
            function (Callable[..., Any]): the function to run for each task.
            results (bool, optional): whether to return the results of the tasks. If True, then
                the method will return a TaskIdentifier that can be used to get the results of the
                tasks. If False, then the method will return None. Defaults to True.
            same_kwargs (dict[str, Any], optional): the keyword arguments that are the same for
                all tasks. Defaults to an empty dict.
            different_kwargs (dict[str, list[Any]], optional): the keyword arguments that are
                different for each task. The keys should be the names of the keyword arguments and
                the values should be lists of the values for each task. Defaults to an empty dict.
            coordinator (ProcessCoordinator | None, optional): the coordinator instance to
                associate with the tasks. Used if you want to do some nested multiprocessing.
                Defaults to None.

        Returns:
            TaskIdentifier | None: the identifier of the group of tasks that were just added to the
                stack. None if 'results' is False.
        """

        # IDENTIFIER
        group_id = self._unique_id.plus() # different int for each task group

        # INDEXes
        valid_values = self._index_tracker.group_tasks(
            number_of_tasks=number_of_tasks,
            nb_of_queues=self._manager_nb[0],
        )

        # SEND to stack(s)
        for stack_index, n in enumerate(valid_values):
            first_index = sum(valid_values[:stack_index])
            last_index = first_index + n - 1
            self._stacks[stack_index].stack.put(
                group_id=group_id,
                total_tasks=number_of_tasks,
                first_task_index=first_index,
                last_task_index=last_index,
                function=function,
                same_kwargs=same_kwargs,
                different_kwargs={
                    k: v[first_index:last_index + 1] for k, v in different_kwargs.items()
                },
                coordinator=coordinator,
                results=results,
            )

        # SETUP queue index trackers
        self._index_tracker.add(number_of_tasks=number_of_tasks)

        # COUNTs (needs to be put after the stack is updated)
        self._stack_count.plus(number_of_tasks)
        self._sorter_count.plus()

        # IDENTIFIER to return group results
        identifier = TaskIdentifier(
            index=0,  # not used
            group_id=group_id,
            total_tasks=number_of_tasks,
            group_tasks=0,
        ) if results else None
        return identifier

    def get(self) -> TaskValue:
        """
        To get a task from an input stack(s).

        Returns:
            TaskValue: the information needed to run the task.
        """

        # COUNT
        self._stack_count.minus()
        
        # STACK choose
        stack_index = self._index_tracker.stack.next()
        return self._stacks[stack_index].stack.get()

    def sort(self, identifier: TaskIdentifier, data: Any) -> None:
        """
        To send a result to one of the sorter managers.

        Args:
            identifier (TaskIdentifier): the identifier of the task that produced the result.
            data (Any): the corresponding task result.
        """

        # GROUP TASKS depends on nb of queues
        valid_values = self._index_tracker.group_tasks(
            number_of_tasks=identifier.total_tasks,
            nb_of_queues=self._manager_nb[1],
        )
        # INDEX
        sorter_index = identifier.index % len(valid_values)
        identifier.group_tasks = valid_values[sorter_index]  # update group tasks

        # SORTER add
        self._sorters[sorter_index].results.put(task_identifier=identifier, data=data)

    def give(self, identifier: TaskIdentifier) -> list[Any]:
        """
        To give back the results of a given task group.
        Keep in mind that before calling this method, the 'full' method should be returning
        True.

        Args:
            finder (GroupFinder): the identifier to a group of tasks that were submitted.

        Returns:
            list[Any]: the results for that group of tasks.
        """

        # COUNT
        self._sorter_count.minus()

        # SORTER choose
        valid_values = self._index_tracker.group_tasks(
            number_of_tasks=identifier.total_tasks,
            nb_of_queues=self._manager_nb[1],
        )

        # GET results
        give: list[tuple[int, Any]] = []
        for sorter_index in range(len(valid_values)):
            give += self._sorters[sorter_index].results.give(identifier=identifier)
        give_sorted = sorted(give, key=lambda x: x[0])  # sort by index
        return [data for _, data in give_sorted]
    
    def check(self) -> bool | None:
        """
        To check if there are tasks in the input stack(s) and if there are results in waiting.
        Hence, it is done to check if the worker process should continue processing tasks or not.

        Returns:
            bool | None: the boolean indicating if there are tasks to process (True) or not
                (False). Returns None when no tasks or results in waiting (i.e. workers need to be
                stopped).
        """

        stack_check = (self._stack_count.minus() <= -1)  # * to act as a lock
        sorter_check = (self._sorter_count.plus(0) == 0)
        if stack_check:
            if sorter_check:
                if self._verbose > 0:
                    print("\033[1;31mAll tasks and results processed.\033[0m", flush=self._flush)
                return None  # all tasks done
            return False  # wait for results
        return True  # stack ready to process

    def full(self, identifier: TaskIdentifier) -> bool:
        """
        To check if all the results for a given group of tasks are ready.

        Args:
            identifier (TaskIdentifier): the identifier to a group of tasks that were submitted.

        Returns:
            bool: True if all the results for that group of tasks are ready, False otherwise.
        """

        # SORTER choose
        valid_values = self._index_tracker.group_tasks(
            number_of_tasks=identifier.total_tasks,
            nb_of_queues=self._manager_nb[1],
        )

        # CHECK results ready
        full = all([
            self._sorters[index].results.full(identifier=identifier)
            for index in range(len(valid_values))
        ])
        return full

    def shutdown(self) -> None: 
        """
        To shutdown the manager(s).
        """

        for stack in self._stacks: stack.manager.shutdown()
        for sorter in self._sorters: sorter.manager.shutdown()

    def _manager_numbers(self, managers: int | tuple[int, int]) -> tuple[int, int]:
        """
        To get the number of stacks and sorters to create.

        Args:
            managers (int | tuple[int, int]): the number of managers to create.

        Raises:
            ValueError: if the number of managers is less than 1.

        Returns:
            tuple[int, int]: the number of stacks and sorters to create.
        """

        if isinstance(managers, int):
            # Need at least one manager for the code to run
            if managers < 1: raise ValueError("Number of managers must be at least 1.")

            if managers == 1:
                # (0, STACK + SORTER)
                manager_nb = (0, 1)
            else:
                # (STACK, SORTERs)
                manager_nb = (1, managers - 1)
        else:
            # (STACKs, SORTERs)
            manager_nb = managers
        return manager_nb

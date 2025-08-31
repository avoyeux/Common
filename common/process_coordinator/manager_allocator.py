"""
To allocate managers to the process coordinator.
This is needed if the size of the data to be sent between workers is large.
In this case multiple managers might be needed to serialize the data and not lead to the workers
not doing anything while waiting for the manager to serialize the data.
"""
from __future__ import annotations

# IMPORTs standard
import time

# IMPORTs local
from .utils import IndexAllocator
from .custom_manager import CustomManager, TaskIdentifier

# TYPE ANNOTATIONs
from typing import Any, Callable, overload, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from .shared_count import Counter
    from .custom_manager.multiprocessing_manager import Results, Stack, TaskValue

# API public
__all__ = ['ManagerAllocator']

# ! code doesn't work with only one manager.



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
            count: Counter,
            manager_nb: tuple[int, int],
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
            count (Counter): the shared memory counter to keep track of the number of tasks and
                results.
            manager_nb (int | tuple[int, int], optional): the number of managers to setup. When an
                integer bigger than one, one manager is created for the input stack and the rest
                are used for sorting the results. When a tuple, the first element is the number of
                managers for the input stack and the second element is the number of managers for
                sorting the results. Defaults to 1.
            verbose (int, optional): the verbosity level for the prints. Defaults to 1.
            flush (bool, optional): whether to flush the output after each print.
                Defaults to False.
        """

        # SHARED values, stack(s), sorter(s)
        self.count = count
        self._stacks: list[StartedStack] = [StartedStack() for _ in range(manager_nb[0])]
        self._sorters: list[StartedSorter] = [StartedSorter() for _ in range(manager_nb[1])]
        if not self._stacks: self._stacks = [self._sorters[0]]

        # ATTRIBUTEs
        self._manager_nb = (manager_nb[0] if manager_nb[0] > 0 else 1, manager_nb[1])
        self._verbose = verbose
        self._flush = flush

    def __getstate__(self) -> dict[str, Any]:
        """
        To get the state of the object for pickling.
        'count' is not pickled as there are locks inside it that shouldn't be pickled.

        Returns:
            dict[str, Any]: the state of the object for pickling.
        """

        state = {
            '_manager_nb': self._manager_nb,
            "_stacks": self._stacks,
            "_sorters": self._sorters,
            "_verbose": self._verbose,
            "_flush": self._flush,
        }
        return state
    
    def __setstate__(self, state: dict[str, Any]) -> None: self.__dict__.update(state)

    @overload
    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[True],
            same_kwargs: dict[str, Any] = ...,
            different_kwargs: dict[str, list[Any]] = ...,
        ) -> TaskIdentifier: ...

    @overload
    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[False],
            same_kwargs: dict[str, Any] = ...,
            different_kwargs: dict[str, list[Any]] = ...,
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
        ) -> TaskIdentifier | None: ...

    def submit(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = True,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
        ) -> TaskIdentifier | None:
        """
        todo update docstring for 'number_of_tasks' != len(different_kwargs.values())) (not always)
        Submits a group of tasks to the input stack(s).

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

        Returns:
            TaskIdentifier | None: the identifier of the group of tasks that were just added to the
                stack. None if 'results' is False.
        """

        # IDENTIFIER
        group_id = self.count.group_id.plus()
        if self._verbose > 0: print(f"ID: {group_id} tasks: {number_of_tasks}", flush=self._flush)

        # CHECK kwargs
        if (
            (not different_kwargs) or 
            ((different_len := len(next(iter(different_kwargs.values())))) == number_of_tasks)
            ):
            valid_values = IndexAllocator.valid_indexes(
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
                    results=results,
                )
        # AGRs partitioning
        else:
            if different_len < number_of_tasks:
                raise ValueError(
                    f"'number_of_tasks' ({number_of_tasks}) is bigger than the length of the "
                    f"lists for the different kwargs (i.e. {different_len}). While it is allowed "
                    "to do the opposite, it is not allowed to have more tasks than different "
                    "kwargs values."
                )
            
            if self._verbose > 2: 
                print(
                    f"\033[91mWarning: 'number of tasks' ({number_of_tasks}) does not match "
                    f"the length of 'different_kwargs' ({different_len}). Slicing "
                    f"'different_kwargs' in sections. {function.__qualname__} will receive a "
                    "list for the 'different_kwargs' keys.\033[0m",
                    flush=self._flush,
                )

            # INDEXes
            tasks_per_stack = IndexAllocator.valid_indexes(
                number_of_tasks=number_of_tasks,
                nb_of_queues=self._manager_nb[0],
            )
            args_per_task = IndexAllocator.valid_indexes(
                number_of_tasks=different_len,
                nb_of_queues=number_of_tasks,
            )  # * could do it for each key value. Better to let user ensure same size lists.

            for stack_index, n in enumerate(tasks_per_stack):
                # TASK INDEXes
                first_index = sum(tasks_per_stack[:stack_index])
                last_index = first_index + n - 1

                # ARGs
                first_args_index = sum(args_per_task[:first_index])
                last_args_index = sum(args_per_task[:last_index + 1]) - 1

                # SUBMIT to stack(s)
                self._stacks[stack_index].stack.put(
                    group_id=group_id,
                    total_tasks=number_of_tasks,
                    first_task_index=first_index,
                    last_task_index=last_index,
                    function=function,
                    same_kwargs=same_kwargs,
                    different_kwargs={
                        k: v[first_args_index:last_args_index + 1]
                        for k, v in different_kwargs.items()
                    },
                    results=results,
                    length_difference=True,
                )

        # IDENTIFIER to return group results
        identifier = TaskIdentifier(
            index=0,  # not used
            group_id=group_id,
            total_tasks=number_of_tasks,
            group_tasks=0,
        ) if results else None

        # COUNTs (needs to be put after the stack is updated)
        self.count.stacks.plus(number_of_tasks)
        self.count.sorters.plus()
        self.count.list.add(total_tasks=number_of_tasks)
        self.count.dict.set(key=group_id, total_tasks=number_of_tasks)
        return identifier

    def get(self) -> TaskValue:
        """
        To get a task from an input stack(s).

        Returns:
            TaskValue: the information needed to run the task.
            
        """

        # COUNT
        self.count.stacks.minus()

        # STACK choose
        stack_index = self.count.list.next()
        result = self._stacks[stack_index].stack.get()
        return result

    def sort(self, identifier: TaskIdentifier, data: Any) -> None:
        """
        To send a result to one of the sorter managers.

        Args:
            identifier (TaskIdentifier): the identifier of the task that produced the result.
            data (Any): the corresponding task result.
        """

        # GROUP TASKS depends on nb of queues
        valid_values = IndexAllocator.valid_indexes(
            number_of_tasks=identifier.total_tasks,
            nb_of_queues=self._manager_nb[1],
        )

        # INDEX
        sorter_index = self.count.dict.get(key=identifier.group_id)
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
        self.count.sorters.minus()

        # SORTER choose
        valid_values = IndexAllocator.valid_indexes(
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
        stack_value = self.count.stacks.minus()  # * acts as a lock
        sorter_value = self.count.sorters.minus(0)
        stack_check = (stack_value <= -1)
        sorter_check = (sorter_value == 0)
        if stack_check:
            if sorter_check: return None  # all tasks done
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
        valid_values = IndexAllocator.valid_indexes(
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

        # SHARED MEMORY
        self.count.close()
        self.count.unlink()

        # MANAGERs
        for stack in self._stacks: stack.manager.shutdown()
        for sorter in self._sorters: sorter.manager.shutdown()

"""
To allocate managers to the process coordinator.
This is needed if the size of the data to be sent between workers is large.
In this case multiple managers might be needed to serialize the data and not lead to the workers
not doing anything while waiting for the manager to serialize the data.
"""
from __future__ import annotations

# IMPORTs local
from .custom_manager import CustomManager

# TYPE ANNOTATIONs
from typing import Any, cast, Callable, Generator, overload, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from multiprocessing.synchronize import Lock as mp_lock
    from .custom_manager.multiprocessing_manager import (
        Results, Integer, Stack, CustomManagerProtocol, TaskIdentifier, StackTracker, TaskValue,
        TaskIdentifier,
    )
    from .task_allocator import ProcessCoordinator


# ! careful of cases where there is only one task left but inside a specific stack

class StartedStack:
    # todo add docstring
    # todo could change this to a dataclass or add slots manually
    # * separating both as the results instance has a thread inside it and don't want to add
    # * unused threads.

    # todo define __slots__

    def __init__(self) -> None:

        manager = CustomManager()
        manager.start()
        self.manager = manager
        self.stack: Stack = self.manager.Stack()

    def __getstate__(self) -> dict[str, Stack]: return {"stack": self.stack}

    def __setstate__(self, state: dict[str, Stack]) -> None: self.__dict__.update(state)

class StartedSorter:
    # todo add docstring
    # todo define __slots__

    def __init__(self) -> None:

        manager = CustomManager()
        manager.start()
        self.manager = manager
        self.results: Results = self.manager.Results()
        self.integer: Integer = self.manager.Integer()
        self.stack: Stack = self.manager.Stack()  # for when there is only one manager total

    def __getstate__(self) -> dict[str, Any]:
        return {
            "results": self.results,
            "integer": self.integer,
            "stack": self.stack,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)

class ManagerAllocator:
    # todo add docstring

    def __init__(
            self,
            managers: int | tuple[int, int] = 1,
            verbose: int = 1,
            flush: bool = False,
        ) -> None:
        # todo add docstring

        # MANAGER arrangement
        self._manager_nb = self._manager_numbers(managers)

        # SHARED stack(s) and sorter(s)
        self._stacks: list[StartedStack] = [StartedStack() for _ in range(self._manager_nb[0])]
        self._sorters: list[StartedSorter] = [StartedSorter() for _ in range(self._manager_nb[1])]
        if not self._stacks: self._stacks = [self._sorters[0]]
        self._stack_tracker: StackTracker = self._stacks[0].manager.StackTracker()

        # SHARED numbers
        self._unique_id: Integer = self._stacks[0].manager.Integer()
        self._stack_count: Integer = self._stacks[0].manager.Integer()
        self._sorter_count: Integer = self._stacks[0].manager.Integer()

        # ATTRIBUTEs settings
        self._verbose = verbose
        self._flush = flush

    def __getstate__(self) -> dict[str, Any]:
        # todo add docstring
        return self.__dict__
    
    def __setstate__(self, state: dict[str, Any]) -> None:
        # todo add docstring
        self.__dict__.update(state)

    @overload
    def submit(
            self,
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
    def submit(
            self,
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
    def submit(
            self,
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

    def submit(
            self,
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
        todo change docstring. This is the one from the ProcessCoordinator.
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

        # COUNTs
        unique_id = self._unique_id.plus()
        self._stack_count.plus(number_of_tasks)
        self._sorter_count.plus()

        # INDEXes
        stack_index = 0 if self._manager_nb[0] == 0 else unique_id % self._manager_nb[0]
        self._stack_tracker.add(stack_index=stack_index, number_of_tasks=number_of_tasks)

        # SEND
        identifier = self._stacks[stack_index].stack.put(
            unique_id=unique_id,
            number_of_tasks=number_of_tasks,
            function=function,
            function_kwargs=function_kwargs,
            coordinator=coordinator,
            results=results,
        )
        return identifier

    def get(self) -> TaskValue:
        # todo add docstring

        # COUNT
        self._stack_count.minus()
        
        # STACK choose
        stack_index = self._stack_tracker.next()
        return self._stacks[stack_index].stack.get()

    def sort(self, identifier: TaskIdentifier, data: Any) -> None:
        # todo add docstring

        # SORTER choose
        sorter_index = identifier.process_id % self._manager_nb[1]
        self._sorters[sorter_index].results.put(task_identifier=identifier, data=data)

    def give(self, identifier: TaskIdentifier) -> list[Any]:
        # todo add docstring

        # todo I need to think about how to check if a result is available

        # COUNT
        self._sorter_count.minus()

        # SORTER choose
        sorter_index = identifier.process_id % self._manager_nb[1]
        return self._sorters[sorter_index].results.give(identifier=identifier)
    
    def check(self) -> bool | None:
        """
        todo lock this from outside (I think ?)
        todo change docstring, this is from another class
        To check if there are tasks in the input stack and if there are results in waiting.
        Hence, it is done to check if the worker process should continue processing tasks or not.

        Args:
            input_stack (List): the stack to get the tasks from.
            results_integer (Integer): integer to track the number of results in waiting.

        Returns:
            bool | None: the boolean indicating if there are tasks to process (True) or not
                (False). 
        """

        stack_check = (self._stack_count.plus(0) == 0)
        sorter_check = (self._sorter_count.plus(0) == 0)
        if stack_check:
            if sorter_check: return None  # all tasks done
            print(f"no stack but results in waiting. sorter_count {self._sorter_count.plus(0)}", flush=True)
            return False  # wait for results
        return True  # stack ready to process

    def full(self, identifier: TaskIdentifier) -> bool:
        # todo add docstring

        # SORTER choose
        sorter_index = identifier.process_id % self._manager_nb[1]
        return self._sorters[sorter_index].results.full(identifier=identifier)

    def shutdown(self) -> None: 
        # todo add docstring

        for stack in self._stacks: stack.manager.shutdown()
        for sorter in self._sorters: sorter.manager.shutdown()

    def _manager_numbers(self, managers: int | tuple[int, int]) -> tuple[int, int]:
        # todo add docstring

        if isinstance(managers, int):
            # Need at least one manager for the code to run
            if managers < 1: raise ValueError("Number of managers must be at least 1.")

            if managers == 1:
                # (0, STACK + SORTER)
                manager_nb = (0, 1)
            else:
                # (STACK, SORTERs)
                manager_nb = (1, managers - 1)  # todo need to check if my prediction is correct
        else:
            # (STACKs, SORTERs)
            manager_nb = managers
        return manager_nb

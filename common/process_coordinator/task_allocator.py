"""
To allocate tasks to processes.
The point of the code is to create the processes only once and use a set number of them
independently of how nested the multiprocessing is and how many tasks you want to run.
"""
from __future__ import annotations

# IMPORTs standard
import time

# IMPORTs alias
import multiprocessing as mp

# IMPORTs local
from .shared_count import Counter
from .manager_allocator import ManagerAllocator

# TYPE ANNOTATIONs
from typing import Any, Self, Callable, overload, Literal, TYPE_CHECKING
if TYPE_CHECKING: from .custom_manager import TaskIdentifier

# API public
all = ['ProcessCoordinator']

# todo need to add a more efficient method if no return value is needed
# ! calling .give() is mandatory in the first submit_task call, otherwise one of the workers is
# ! lost.



class ProcessCoordinator:
    """
    todo update docstring
    Creates a set number of processes once and uses them to run tasks.
    Nested multiprocessing is supported by passing the coordinator instance to the workers.
    Keep in mind that the target function will need a 'coordinator' if wanting to use nested
    multiprocessing.
    """

    def __init__(
            self,
            workers: int = 2,
            managers: int | tuple[int, int] = 1,
            verbose: int = 1,
            flush: bool = False,
        ) -> None:
        """
        Initializes the ProcessCoordinator with a set number of processes.
        It will create a set number of processes once and uses them to run tasks.
        Nested multiprocessing is supported by passing the coordinator instance to the workers.
        Keep in mind that the target function will need a 'coordinator' if wanting to use nested
        multiprocessing.
        
        Args:
            workers (int): the max number of processes to create. Defaults to 2.
            managers (int | tuple[int, int], optional): the number of managers to create.
                If a single integer is passed, then it will create that many managers with
                one input stack and one results stack. If a tuple is passed, then it will create
                that many managers with the first element being the number of input stacks and
                the second element being the number of results stacks. Defaults to 1.
            verbose (int, optional): the verbosity level for the prints. Defaults to 1.
            flush (bool, optional): whether to flush the output. Defaults to False.
        """

        # ATTRIBUTEs from args
        self._total_processes = workers - 1  # * main process also acts as a worker hence -1

        # ATTRIBUTEs settings
        self._verbose = verbose
        self._flush = flush

        # SETUP manager
        manager = ManagerAllocator(managers=managers, verbose=self._verbose - 1, flush=self._flush)
        self._manager = manager

        # SHARED counts
        self.count = Counter(managers_nb=manager.manager_nb, length=1024)

        # CREATE processes
        self._processes: list[mp.Process] | None = self._create_processes()

    def __getstate__(self) -> dict[str, Any]:
        """
        To get the state (serialization) of the ProcessCoordinator instance.

        Returns:
            dict[str, Any]: the state to keep of the ProcessCoordinator instance.
        """

        state = {
            "_verbose": self._verbose,
            "_flush": self._flush,
            "_manager": self._manager,
            "_processes": None,
        }
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        To set the state (deserialization) of the ProcessCoordinator instance.

        Args:
            state (dict[str, Any]): _description_
        """

        self.__dict__.update(state)

    def __enter__(self) -> Self:
        """
        To enter the context manager.

        Returns:
            Self: the process coordinator instance.
        """

        return self

    def __exit__(self, exc_type: Any = None, exc_value: Any = None, traceback: Any = None) -> None:
        """
        To exit the context manager and clean up the resources.
        The arguments are not used, hence why their explanation is not in the docstring.
        """

        # STOP processes
        for p in self._processes: p.join()  # * cannot actually be None here.

        # CLOSE manager
        self._manager.shutdown(self.count)

    def exit(self) -> None:
        """
        To exit the ProcessCoordinator and clean up the resources.
        Used if you want to exit the ProcessCoordinator without using the context manager.
        """

        self.__exit__()

    @overload
    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[True] = ...,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier: ...
    
    @overload
    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[False],
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            coordinator: ProcessCoordinator | None = ...,
        ) -> None: ...
    
    # FALLBACK
    @overload
    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: bool = ...,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            coordinator: ProcessCoordinator | None = ...,
        ) -> TaskIdentifier | None: ...

    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable,
            results: bool = True,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
            coordinator: ProcessCoordinator | None = None,
        ) -> TaskIdentifier| None:
        """
        To submit a group of tasks to the input stack(s).
        If you are planning to also submit tasks inside this call, then you should pass the
        'ProcessCoordinator' instance to the function. Make sure that 'function' has a 
        'coordinator' keyword argument for the nested multiprocessing to work.

        Args:
            number_of_tasks (int): the number of tasks to submit.
            function (Callable): the function to run for each task.
            results (bool, optional): whether to return the results of the tasks. Defaults to True.
            same_kwargs (dict[str, Any], optional): the keyword arguments that are the same for all
                tasks. Defaults to {}.
            different_kwargs (dict[str, list[Any]], optional): the keyword arguments that are
                different for each task. The keys are the names of the keyword arguments and the
                values are lists of values where each item is a value for a specific task.
                Defaults to {}.
            coordinator (ProcessCoordinator | None, optional): the coordinator to pass to the
                function. Only do so if you want to do some nested multiprocessing.
                Defaults to None.
        
        Returns:
            TaskIdentifier: the identifier of the task(s) that was(were) just submitted.
        """

        # SEND to manager
        identifier = self._manager.submit(
            count=self.count,
            number_of_tasks=number_of_tasks,
            function=function,
            results=results,
            same_kwargs=same_kwargs,
            different_kwargs=different_kwargs,
            coordinator=coordinator,
        )

        # PROCESSEs start
        if self._processes is not None:
            for p in self._processes: p.start()
        return identifier
    
    def give(self, identifier: TaskIdentifier) -> list[Any]:
        """
        To get the results of a group of tasks.
        If the results are not ready yet, it will run another task to completion and get back to
        waiting for the results. If, again, they are not ready, here we go again.

        Args:
            task_identifier (TaskIdentifier): the identifier unique to each task sent to the
                manager.

        Returns:
            list[Any]: ordered data list of the results of a given group of tasks.
        """
        
        # CHECK if results ready
        while not self._manager.full(identifier):

            # NEW task processing
            if self._single_worker_process(): time.sleep(1)

        # READY to get results
        results = self._manager.give(identifier)
        return results

    def _create_processes(self) -> list[mp.Process]:
        """
        To create the processes that will run the tasks.

        Returns:
            list[mp.Process]: the list of created processes. Returned as .start() and .join() are
            called on them later.
        """

        # PROCESS kwargs
        kwargs = {'manager': self._manager, 'count': self.count}

        # CREATE processes
        processes = [
            mp.Process(target=self._worker_process, kwargs=kwargs)
            for _ in range(self._total_processes)
        ]  # * start done later to make sure that there are always tasks being done.
        return processes

    @staticmethod
    def _worker_process(manager: ManagerAllocator, count: Counter) -> None:
        """
        todo update docstring
        To run the worker process that will fetch the tasks from the input stack and put the
        results into the results stack.

        Args:
            input_stack (List): the stack to get the tasks from.
            results (Results): the output to sort the results.
            input_integer (Integer): the integer to track the number of tasks in the input stack.
            results_integer (Integer): the integer to track the number of results in waiting.
            manager_lock (mp_lock): the lock to synchronize access to the manager.
        """

        while True:
            # CHECK input
            check = manager.check(count)
            if check: fetch = manager.get(count) # input ready

            # COUNT tasks
            count.stacks.plus()  # * acts as a lock.release()

            if check is None: 
                print(f"\033[1;31mExiting a worker\033[0m", flush=manager._flush)
                break  # all tasks done
            if not check: time.sleep(1); continue # wait for more tasks

            # FETCH task
            fetch, coordinator, result = fetch
            if coordinator is not None: 
                coordinator.count = count
                fetch.kwargs['coordinator'] = coordinator

            # PROCESS run
            output = ''  # for results to never be undefined
            try:
                output = fetch.function(**fetch.kwargs)
            except BaseException as e:
                output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
            finally:
                # PROCESS output
                if result: manager.sort(count=count, identifier=fetch.identifier, data=output)
        count.close()

    def _single_worker_process(self) -> bool:
        """
        To run a single worker process that will fetch on task from the input stack and put the
        result into the results stack.

        Returns:
            bool: True if the input stack is empty, False otherwise.
        """

        # CHECK input
        check = self._manager.check(self.count)
        if check: fetch = self._manager.get(self.count)  # input ready

        # COUNT tasks
        self.count.stacks.plus()  # * ~ .release()

        if check is None:
            print(
                "\033[1;31mERROR: no tasks and results left. Shouldn't happen here. "
                "Raising ValueError to stop the process. The post processing will not work if "
                "a return value is expected. \033[0m",
                flush=self._flush,
            )
            raise ValueError("Problem: no results left where there should be.")
        if not check: return True  # wait for results

        # FETCH task
        fetch, coordinator, result = fetch
        if coordinator is not None:
            coordinator.count = self.count
            fetch.kwargs['coordinator'] = coordinator

        # PROCESS run
        output = ''  # for results to never be undefined
        try:
            output = fetch.function(**fetch.kwargs)
        except BaseException as e:
            output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
        finally:
            # PROCESS output
            if result:
                self._manager.sort(count=self.count, identifier=fetch.identifier, data=output)
            return False



if __name__ == "__main__":
    from common import Decorators, ProcessCoordinator
    from typing import Any

    @Decorators.running_time(flush=True)
    def main_worker(x: int, coordinator: ProcessCoordinator) -> list[Any]:
        task_id = coordinator.submit_tasks(
            number_of_tasks=5,
            function=task_function,
            same_kwargs={"x": x},
            different_kwargs={"y": [i for i in range(5)]},
            results=True,
        )
        
        # Wait for results
        results = coordinator.give(task_id)
        print(f"results for task {x}: {results}", flush=True)
        return results

    def task_function(x: int, y: int) -> tuple[int, int]:
        """
        A simple task function to test the ProcessCoordinator.
        """
        [None for _ in range(10000) for j in range(1000)]  # Simulate some work
        [None for _ in range(10000) for j in range(1000)]  # Simulate some work

        # print(f"Task {x} - {y} done", flush=True)
        return (x, y)

    @Decorators.running_time(flush=True)
    def run():
        with ProcessCoordinator(workers=4, managers=(2, 2), verbose=3, flush=True) as coordinator:
            task_id = coordinator.submit_tasks(
                number_of_tasks=5,
                function=main_worker,
                different_kwargs={"x": [i for i in range(5)]},
                coordinator=coordinator,
            )
            
            # Wait for results
            results = coordinator.give(task_id)
            print(f'Final results: {results}', flush=True)
        print("ProcessCoordinator exited cleanly.")
    run()

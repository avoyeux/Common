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
    Singleton class that creates a set number of processes once and uses them to run tasks.
    Nested multiprocessing is supported by calling the ProcessCoordinator class again (as it is a 
    singleton).
    """

    # CLASS ATTRIBUTEs singleton
    _instance: ProcessCoordinator | None = None
    _initialized: bool = False
    _process_started: bool = False

    def __new__(cls, *args, **kwargs) -> ProcessCoordinator:
        """
        To create a new instance of the ProcessCoordinator singleton.

        Returns:
            ProcessCoordinator: the singleton instance of the ProcessCoordinator.
        """

        if cls._instance is None: cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self,
            workers: int = 2,
            managers: int | tuple[int, int] = 1,
            verbose: int = 1,
            flush: bool = False,
        ) -> None:
        """
        Initializes the ProcessCoordinator singleton with a set number of processes.
        If ProcessCoordinator was already initialized, it will not reinitialise.
        When initialised, it creates a set number of processes once and uses them to run tasks.
        Nested multiprocessing is supported and can be done just by calling the ProcessCoordinator
        class again (as it is a singleton). The given instance will be the 'same' as the main one.
        
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

        # SINGLETON check
        if self._initialized: return

        # ATTRIBUTEs from args
        self._total_processes = workers - 1  # * main process also acts as a worker hence -1

        # ATTRIBUTEs settings
        self._verbose = verbose
        self._flush = flush

        # SETUP manager
        manager_nb = self._manager_numbers(managers)
        self.count = Counter(managers_nb=manager_nb, length=256)
        manager = ManagerAllocator(
            count=self.count,
            manager_nb=manager_nb,
            verbose=self._verbose - 1,
            flush=self._flush,
        )
        self.manager = manager

        # CREATE processes
        self._processes: list[mp.Process] | None = self._create_processes()

        # SINGLETON initialized
        self._initialized = True

    def __getstate__(self) -> dict[str, Any]:
        """
        To get the state (serialization) of the ProcessCoordinator instance.

        Returns:
            dict[str, Any]: the state to keep of the ProcessCoordinator instance.
        """

        state = {
            "_total_processes": self._total_processes,
            "manager": self.manager,
            "_processes": None,
            "_instance": None,
            "_initialized": False,
            "_verbose": self._verbose,
            "_flush": self._flush,
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
        self.manager.shutdown()

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
        ) -> TaskIdentifier: ...
    
    @overload
    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable[..., Any],
            results: Literal[False],
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
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
        ) -> TaskIdentifier | None: ...

    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable,
            results: bool = True,
            same_kwargs: dict[str, Any] = {},
            different_kwargs: dict[str, list[Any]] = {},
        ) -> TaskIdentifier| None:
        """
        todo update docstring for 'number_of_tasks' != len(different_kwargs.values()) (not always)
        To submit a group of tasks to the input stack(s).

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
        
        Returns:
            TaskIdentifier: the identifier of the task(s) that was(were) just submitted.
        """

        # SEND to manager
        self.manager.count = self.count  # ! do I still need this?
        identifier = self.manager.submit(
            number_of_tasks=number_of_tasks,
            function=function,
            results=results,
            same_kwargs=same_kwargs,
            different_kwargs=different_kwargs,
        )

        # PROCESSEs start
        if self._processes is not None and not self._process_started:
            for p in self._processes: p.start()
            self._process_started = True
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
        while not self.manager.full(identifier):

            # NEW task processing
            if self._single_worker_process():
                if self._verbose > 1: print("\033[90mWaiting full...\033[0m", flush=True)
                time.sleep(0.5)

        # READY to get results
        results = self.manager.give(identifier)
        return results

    def _create_processes(self) -> list[mp.Process]:
        """
        To create the processes that will run the tasks.

        Returns:
            list[mp.Process]: the list of created processes. Returned as .start() and .join() are
            called on them later.
        """

        # PROCESS kwargs
        kwargs = {'process_coordinator': self, 'count': self.count}

        # CREATE processes
        processes = [
            mp.Process(target=self._worker_process, kwargs=kwargs)
            for _ in range(self._total_processes)
        ]  # * start done later to make sure that there are always tasks being done.
        return processes

    @staticmethod
    def _worker_process(process_coordinator: ProcessCoordinator, count: Counter) -> None:
        """
        To run the worker process that will fetch the tasks from the input stack and put the
        results into the results stack.
        Also repopulates the ProcessCoordinator singleton instance so that the worker processes
        can access the 'same' instance as the main process by just calling ProcessCoordinator().

        Args:
            process_coordinator (ProcessCoordinator): the ProcessCoordinator instance gotten from
                the main process.
            count (Counter): the shared memory counters to keep track of the number of tasks and
                results.
        """

        # INSTANCE repopulate
        process_coordinator = ProcessCoordinator._repopulate_self(process_coordinator, count)

        while True:
            # CHECK input
            check = process_coordinator.manager.check()
            if check: fetch = process_coordinator.manager.get() # input ready

            # COUNT tasks
            count.stacks.plus()  # * acts as a lock.release()

            if check is None: 
                print(
                    f"\033[1;31mExiting a worker\033[0m",
                    flush=process_coordinator.manager._flush,
                )
                break  # all tasks done
            if not check:
                if process_coordinator._verbose > 1: print("\033[90mWaiting...\033[0m", flush=True)
                time.sleep(0.5)
                continue # wait for more tasks

            # FETCH task
            fetch, result = fetch

            # PROCESS run
            output = ''  # for results to never be undefined
            try:
                output = fetch.function(**fetch.kwargs)
            except BaseException as e:
                output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
            finally:
                # PROCESS output
                if result: process_coordinator.manager.sort(identifier=fetch.identifier, data=output)
        count.close()

    @staticmethod
    def _repopulate_self(
            process_coordinator: ProcessCoordinator,
            count: Counter,
        ) -> ProcessCoordinator:
        """
        To repopulate the ProcessCoordinator singleton instance so that the worker processes can
        access the 'same' instance as the main process by just calling ProcessCoordinator().

        Args:
            process_coordinator (ProcessCoordinator): the ProcessCoordinator instance gotten from
                the main process.
            count (Counter): the counters used inside the ProcessCoordinator. Passed like so as
                that class has Locks and shared memories that can only be inherited.

        Returns:
            ProcessCoordinator: the repopulated ProcessCoordinator instance.
        """

        # INSTANCE repopulate
        ProcessCoordinator._instance = process_coordinator
        ProcessCoordinator._instance._initialized = True
        ProcessCoordinator._instance._process_started = True

        # COUNT repopulate
        ProcessCoordinator._instance.count = count
        ProcessCoordinator._instance.manager.count = count
        return ProcessCoordinator()

    def _single_worker_process(self) -> bool:
        """
        To run a single worker process that will fetch on task from the input stack and put the
        result into the results stack.

        Returns:
            bool: True if the input stack is empty, False otherwise.
        """

        # CHECK input
        check = self.manager.check()
        if check: fetch = self.manager.get()  # input ready

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
        fetch, result = fetch

        # PROCESS run
        output = ''  # for results to never be undefined
        try:
            output = fetch.function(**fetch.kwargs)
        except BaseException as e:
            output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
        finally:
            # PROCESS output
            if result:
                self.manager.sort(identifier=fetch.identifier, data=output)
            return False

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



if __name__ == "__main__":
    from common import Decorators, ProcessCoordinator
    from typing import Any

    @Decorators.running_time(flush=True)
    def main_worker(x: list[int]) -> list[Any]:
        coordinator = ProcessCoordinator()

        res = []
        for val in x:
            task_id = coordinator.submit_tasks(
                number_of_tasks=5,
                function=task_function,
                same_kwargs={"x": val},
                different_kwargs={"y": [i for i in range(5)]},
                results=True,
            )
            
            # Wait for results
            results = coordinator.give(task_id)
            print(f"results for task {val}: {results}", flush=True)
            res.append(results)
        return res

    def task_function(x: int, y: int) -> tuple[int, int]:
        """
        A simple task function to test the ProcessCoordinator.
        """

        [None for _ in range(10000) for j in range(1000)]  # Simulate some work
        # [None for _ in range(10000) for j in range(1000)]  # Simulate some work
        # print(f"Task {x} - {y} done", flush=True)
        return (x, y)

    @Decorators.running_time(flush=True)
    def run():
        with ProcessCoordinator(workers=8, managers=(3, 3), verbose=5, flush=True) as coordinator:
            task_id = coordinator.submit_tasks(
                number_of_tasks=5,
                function=main_worker,
                different_kwargs={"x": [i for i in range(10)]},
            )
            
            # Wait for results
            results = coordinator.give(task_id)
            print(f'Done indexes are: {[res[0][0] for sublist in results for res in sublist]}', flush=True)
        print("ProcessCoordinator exited cleanly.")
    run()

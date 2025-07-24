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
from .custom_manager import CustomManager, TaskIdentifier, FetchInfo

# TYPE ANNOTATIONs
from typing import Any, Self, Callable, TYPE_CHECKING
if TYPE_CHECKING:
    from multiprocessing.synchronize import Lock as mp_lock
    from .custom_manager.multiprocessing_manager import Results, Integer, List

# API public
all = ['ProcessCoordinator']

# todo need to add a more efficient method if no return value is needed
# ! right now calling '.results()' when nested multiprocessing is mandatory regardless of the need



class ProcessCoordinator:
    """
    Creates a set number of processes once and uses them to run tasks.
    Nested multiprocessing is supported by passing the coordinator instance to the workers.
    Keep in mind that the target function will need a 'coordinator' if wanting to use nested
    multiprocessing.
    """

    def __init__(self, total_processes: int, verbose: int = 1, flush: bool = False) -> None:
        """
        Initializes the ProcessCoordinator with a set number of processes.
        It will create a set number of processes once and uses them to run tasks.
        Nested multiprocessing is supported by passing the coordinator instance to the workers.
        Keep in mind that the target function will need a 'coordinator' if wanting to use nested
        multiprocessing.
        
        Args:
            total_processes (int): the max number of processes to create.
            verbose (int, optional): the verbosity level for the prints. Defaults to 1.
            flush (bool, optional): whether to flush the output. Defaults to False.
        """

        # ATTRIBUTEs from args
        self._total_processes = total_processes - 1  # * keeping one for the manager

        # ATTRIBUTEs settings
        self._verbose = verbose
        self._flush = flush

        # SETUP manager
        manager = CustomManager()
        manager.start()
        self._manager = manager
        self._manager_lock = self._manager.Lock()
        self._input_stack: List = self._manager.List()
        self._results = self._manager.Results()
        self._input_integer = self._manager.Integer()
        self._integer_results = self._manager.Integer()

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
            "_input_stack": self._input_stack,
            "_results": self._results,
            "_input_integer": self._input_integer,
            "_integer_results": self._integer_results,
            "_manager_lock": self._manager_lock,
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
        self._manager.shutdown()

    def exit(self) -> None:
        """
        To exit the ProcessCoordinator and clean up the resources.
        Used if you want to exit the ProcessCoordinator without using the context manager.
        """

        self.__exit__()

    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable,  # ? change it to list[Callable] ?
            function_kwargs: dict[str, Any] | list[dict[str, Any]] = {},
            coordinator: ProcessCoordinator | None = None,
        ) -> TaskIdentifier:
        """
        To submit a task to the input stack.
        If you are planning to also submit tasks inside this call, then you should pass the
        'ProcessCoordinator' instance to the function. Make sure that 'function' has a 
        'coordinator' keyword argument for the nested multiprocessing to work.

        Args:
            number_of_tasks (int): the number of tasks to submit.
            function (Callable): the function to run for each task.
            function_kwargs (dict[str, Any] | list[dict[str, Any]], optional):
                the keyword arguments to pass to the function. If a list is given, then each task
                will have its own set of keyword arguments. Defaults to {}.
            coordinator (ProcessCoordinator | None, optional): the coordinator to pass to the
                function. Only do so if you want to do some nested multiprocessing.
                Defaults to None.
        
        Returns:
            TaskIdentifier: the identifier of the task(s) that was(were) just submitted.
        """

        # COUNT group of tasks
        self._manager_lock.acquire()
        self._integer_results.plus()  # count nb of results in waiting
        self._manager_lock.release()
        
        # SEND tasks
        process_id = self._input_integer.next()  # unique identifier for the task group
        if isinstance(function_kwargs, dict):
            # SUBMIT tasks
            for i in range(number_of_tasks):
                task_input = FetchInfo(
                    identifier=TaskIdentifier(
                        index=i,
                        number_tasks=number_of_tasks,
                        process_id=process_id,
                    ),
                    function=function,
                    kwargs=function_kwargs,
                )

                # COUNT n SEND
                self._manager_lock.acquire()
                self._input_integer.plus()  # count nb of tasks in stack
                self._input_stack.put((task_input, coordinator))
                self._manager_lock.release()
        else:
            # CHECK number
            if not self._check_submit(number_of_tasks, function_kwargs):
                if self._verbose > 0:
                    print(
                        f"\033[1;31mError: number of tasks ({number_of_tasks}) does not match the "
                        f"number of function kwargs ({len(function_kwargs)}). Using the number of "
                        "arguments as the number of tasks.\033[0m",
                        flush=self._flush,
                    )
                number_of_tasks = len(function_kwargs)
            
            # SUBMIT tasks
            for i, kwargs in enumerate(function_kwargs):
                task_input = FetchInfo(
                    identifier=TaskIdentifier(
                        index=i,
                        number_tasks=number_of_tasks,
                        process_id=process_id,
                    ),
                    function=function,
                    kwargs=kwargs,
                )

                # COUNT n SEND
                self._manager_lock.acquire()
                self._input_integer.plus()  # count nb of tasks in stack
                self._input_stack.put((task_input, coordinator))
                self._manager_lock.release()
        
        # PROCESSEs start
        if self._processes is not None:
            for p in self._processes: p.start()

        # IDENTIFIER to get the right results later
        task_identifier = TaskIdentifier(
            index=0,
            number_tasks=number_of_tasks,
            process_id=process_id,
        )
        return task_identifier
    
    def results(self, task_identifier: TaskIdentifier) -> list[Any]:
        """
        To get the results of a group of tasks.
        If the results are not ready yet, it will run another task to completion and get back to
        waiting for the results. If there again not ready, here we go again.

        Args:
            task_identifier (TaskIdentifier): the identifier unique to each task sent to the
                manager.

        Returns:
            list[Any]: ordered data list of the results of a given group of tasks.
        """
        
        # CHECK if results ready
        while not self._results.results_full(task_identifier):

            # CHECK left tasks
            # if self._integer.get() <= 0: print('done', flush=True); break

            # NEW task processing
            if self._single_worker_process(
                input_stack=self._input_stack,
                results=self._results,
                input_integer=self._input_integer,
                results_integer=self._integer_results,
                manager_lock=self._manager_lock,
                ):
                time.sleep(1)

        # READY to get results
        results = self._results.results(task_identifier)
        self._manager_lock.acquire()
        self._integer_results.minus()
        self._manager_lock.release()
        return results

    def _create_processes(self) -> list[mp.Process]:
        """
        To create the processes that will run the tasks.

        Returns:
            list[mp.Process]: the list of created processes. Returned as .start() and .join() are
            called on them later.
        """

        # PROCESS kwargs
        kwargs = {
            'input_stack': self._input_stack,
            'results': self._results,
            'input_integer': self._input_integer,
            'results_integer': self._integer_results,
            'manager_lock': self._manager_lock,
        }

        # CREATE processes
        processes = [
            mp.Process(target=self._worker_process, kwargs=kwargs)
            for _ in range(self._total_processes)
        ]  # * no need to start before finishing creation as jobs won't be submitted before
        return processes

    @staticmethod
    def _worker_process(
            input_stack: List,
            results: Results,
            input_integer: Integer,
            results_integer: Integer,
            manager_lock: mp_lock,
        ) -> None:
        """
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
            manager_lock.acquire()
            check = ProcessCoordinator._check_input_n_outputs(input_integer, results_integer)
            if check:
                fetch = input_stack.get() # input ready
                input_integer.minus()  # count down the number of tasks in stack
            manager_lock.release()
            if check is None: return  # all tasks done
            if not check: time.sleep(1); continue # wait for more tasks

            # FETCH task
            fetch, coordinator = fetch
            if coordinator is not None: fetch.kwargs['coordinator'] = coordinator

            # PROCESS run
            output = ''  # for results to never be undefined
            try:
                output = fetch.function(**fetch.kwargs)
            except BaseException as e:
                output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
            finally:
                # PROCESS output
                results.put(
                    task_identifier=fetch.identifier,
                    data=output,
                )
    
    @staticmethod
    def _check_input_n_outputs(input_integer: Integer, results_integer: Integer) -> bool | None:
        """
        To check if there are tasks in the input stack and if there are results in waiting.
        Hence, it is done to check if the worker process should continue processing tasks or not.

        Args:
            input_integer (Integer): integer to track the number of tasks in the input stack.
            results_integer (Integer): integer to track the number of results in waiting.

        Returns:
            bool | None: the boolean indicating if there are tasks to process (True) or not
                (False). 
        """

        input_value = input_integer.get()
        result_value = results_integer.get()
        if input_value <= 0:
            if result_value <= 0: return None  # all tasks done
            return False  # wait for results
        return True  # input ready to process

    @staticmethod
    def _single_worker_process(
            input_stack: List,
            results: Results,
            input_integer: Integer,
            results_integer: Integer,
            manager_lock: mp_lock,
        ) -> bool:
        """
        To run a single worker process that will fetch on task from the input stack and put the
        result into the results stack.

        Args:
            input_stack (List): the stack to get the tasks from.
            results (Results): the output stack to sort the results.
            input_integer (Integer): the integer to track the number of tasks in the input stack.
            results_integer (Integer): the integer to track the number of results in waiting.
            manager_lock (mp_lock): the lock to synchronize access to the manager.

        Returns:
            bool: True if the input stack is empty, False otherwise.
        """

        # CHECK input
        manager_lock.acquire()
        check = ProcessCoordinator._check_input_n_outputs(input_integer, results_integer)
        if check:
            fetch = input_stack.get()  # input ready
            input_integer.minus()
        manager_lock.release()
        if check is None: raise ValueError("Problem: no results left where there should be.")
        if not check: return True  # wait for results

        # FETCH task
        fetch, coordinator = fetch
        if coordinator is not None: fetch.kwargs['coordinator'] = coordinator

        # PROCESS run
        output = ''  # for results to never be undefined
        try:
            output = fetch.function(**fetch.kwargs)
        except BaseException as e:
            output = f"\033[1;31mException: {type(e).__name__}: {e}\033[0m"
        finally:
            # PROCESS output
            results.put(
                task_identifier=fetch.identifier,
                data=output,
            )
            return False

    def _check_submit(self, number_of_tasks: int, function_kwargs: list[dict[str, Any]]) -> bool:
        """
        To check if the number of tasks and function kwargs match.

        Args:
            number_of_tasks (int): the number of tasks to submit.
            function_kwargs (list[dict[str, Any]]): the function keyword arguments to pass to the
                function for each task.

        Returns:
            bool: true if the number of tasks and function kwargs match, false otherwise.
        """

        if number_of_tasks != len(function_kwargs): return False
        return True



if __name__ == "__main__":
    from common import Decorators

    @Decorators.running_time
    def main_worker(x: int, coordinator: ProcessCoordinator) -> list[Any]:
        task_id = coordinator.submit_tasks(
            number_of_tasks=50,
            function=task_function,
            function_kwargs=[{"x": x, "y": i} for i in range(50)]
        )
        
        # Wait for results
        results = coordinator.results(task_id)
        return results

    def task_function(x: int, y: int) -> tuple[int, int]:
        """
        A simple task function to test the ProcessCoordinator.
        """
        [None for _ in range(10000) for j in range(1000)]  # Simulate some work

        return (x, y)

    @Decorators.running_time
    def run():
        with ProcessCoordinator(total_processes=14) as coordinator:
            task_id = coordinator.submit_tasks(
                number_of_tasks=50,
                function=main_worker,
                function_kwargs=[{"x": i} for i in range(50)], 
                coordinator=coordinator,
            )
            
            # Wait for results
            results = coordinator.results(task_id)
            print(f'Final results: {results}', flush=True)
        print("ProcessCoordinator exited cleanly.")
    run()

"""
To allocate tasks to processes.
The point of the code is to create the processes only once and use a set number of them
independently of how nested the multiprocessing is and how many tasks you want to run.
"""
from __future__ import annotations

# IMPORTs standard
import os

# IMPORTs alias
import multiprocessing as mp

# IMPORTs local
from .custom_manager import CustomManager, TaskIdentifier, FetchInfo

# TYPE ANNOTATIONs
from typing import Any, Self, Callable
from .custom_manager.multiprocessing_manager import Results, CustomQueue

# API public
all = ['ProcessCoordinator']



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
        self._input_queue: CustomQueue = self._manager.Queue()
        self.results = self._manager.Results()

        # CREATE processes
        self._processes = self._create_processes()

    def __getstate__(self) -> dict[str, Any]:
        """
        To get the state (serialization) of the ProcessCoordinator instance.

        Returns:
            dict[str, Any]: the state to keep of the ProcessCoordinator instance.
        """

        state = {
            "_verbose": self._verbose,
            "_flush": self._flush,
            "_input_queue": self._input_queue,
            "results": self.results,
        }
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        To set the state (deserialization) of the ProcessCoordinator instance.

        Args:
            state (dict[str, Any]): _description_
        """

        self.__dict__.update(state)

    def _create_processes(self) -> list[mp.Process]:
        """
        To create the processes that will run the tasks.

        Returns:
            list[mp.Process]: the list of created processes. Returned as .join() will be called on
                them later.
        """

        # CREATE processes
        processes = [
            mp.Process(target=self._worker_process, args=(self._input_queue, self.results))
            for _ in range(self._total_processes)
        ]  # * no need to start before finishing creation as jobs won't be submitted before
        for p in processes: p.start()
        return processes

    @staticmethod
    def _worker_process(input_queue: CustomQueue, results: Results) -> None:
        """
        To run the worker process that will fetch the tasks from the input queue and put the
        results into the results queue.

        Args:
            input_queue (queue.Queue[FetchInfo | None]): the queue to get the tasks from.
            results (Results): the output queue to sort the results.
        """

        while True:
            # CHECK input
            fetch = input_queue.get()
            if fetch is None: return
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

    def submit_tasks(
            self,
            number_of_tasks: int,
            function: Callable,  # ? change it to list[Callable] ?
            function_kwargs: dict[str, Any] | list[dict[str, Any]] = {},
            coordinator: ProcessCoordinator | None = None,
        ) -> TaskIdentifier:
        """
        To submit a task to the input queue.

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
        
        process_id = os.getpid()
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
                self._input_queue.put((task_input, coordinator))
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
                self._input_queue.put((task_input, coordinator))

        # IDENTIFIER to get the right results later
        task_identifier = TaskIdentifier(
            index=0,
            number_tasks=number_of_tasks,
            process_id=process_id,
        )
        return task_identifier
    
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
        for _ in range(self._total_processes): self._input_queue.put(None)
        for p in self._processes: p.join()

        # CLOSE manager
        self._manager.shutdown()

    def exit(self) -> None:
        """
        To exit the ProcessCoordinator and clean up the resources.
        Used if you want to exit the ProcessCoordinator without using the context manager.
        """

        self.__exit__()



if __name__ == "__main__":

    def main_worker(x: int, coordinator: ProcessCoordinator) -> list[Any]:
        task_id = coordinator.submit_tasks(
            number_of_tasks=80,
            function=task_function,
            function_kwargs=[{"x": x, "y": i} for i in range(80)]
        )
        
        # Wait for results
        results = coordinator.results.results(task_id)
        return results

    def task_function(x: int, y: int) -> tuple[int, int]:
        """
        A simple task function to test the ProcessCoordinator.
        """
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]
        [None for _ in range(1000) for i in range(1000)]

        print(f"task_function: x={x}, y={y}", flush=True)
        return (x, y)

    def run():
        with ProcessCoordinator(total_processes=10) as coordinator:
            task_id = coordinator.submit_tasks(
                number_of_tasks=7,
                function=main_worker,
                function_kwargs=[{"x": i} for i in range(7)], 
                coordinator=coordinator,
            )
            
            # Wait for results
            results = coordinator.results.results(task_id)
            print(f'Final results: {results}', flush=True)
        print("ProcessCoordinator exited cleanly.")
    run()

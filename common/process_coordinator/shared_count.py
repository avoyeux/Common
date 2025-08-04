"""
Contains shared memory objects to be used in the process coordinator.
"""
from __future__ import annotations

# IMPORTs standard
import ctypes

# IMPORTs local
from .custom_shared_memory import SharedDict, SharedList, SharedValue

# TYPE ANNOTATIONs
from typing import Any

# API public
__all__ = ["Counter"]



class Counter:
    """
    A class to store shared memory objects.
    # ! not picklable because of the locks.
    """

    def __init__(self, managers_nb: tuple[int, int], length: int = 1024) -> None:
        """
        Initialise the shared memory objects for the process coordinator.
        Designed to be pickled and unpickled across processes.

        Args:
            managers_nb (tuple[int, int]): the number of stacks and sorters.
            length (int, optional): the length of the custom shared memory objects. The length need
                to be a power of 2. Defaults to 1024.
        """

        self._close: bool = False

        self.group_id: SharedValue = SharedValue()
        self.stacks: SharedValue = SharedValue(ctype=ctypes.c_int64)
        self.sorters: SharedValue = SharedValue()
        self.list: SharedList = SharedList(length=length, nb_of_queues=managers_nb[0])
        self.dict: SharedDict = SharedDict(length=length, nb_of_queues=managers_nb[1])

    def __getstate__(self) -> dict[str, Any]: return self.__dict__
    def __setstate__(self, state: dict[str, Any]) -> None: self.__dict__.update(state)

    def close(self) -> None:
        """
        Closing the shared memory objects.
        """

        if self._close: return

        self._close = True
        self.group_id.close()
        self.stacks.close()
        self.sorters.close()
        self.list.close()
        self.dict.close()

    def unlink(self) -> None:
        """
        Unlinking the shared memory objects.
        """

        self.group_id.unlink()
        self.stacks.unlink()
        self.sorters.unlink()
        self.list.unlink()
        self.dict.unlink()



if __name__ == "__main__":

    import multiprocessing as mp
    from common import Decorators
    from typing import cast
    from common.process_coordinator.shared_count import Counter

    count = Counter(managers_nb=(2, 2), length=1024)

    @Decorators.running_time
    def worker_plus(count: Counter) -> None:
        group_id = count.group_id.plus()
        stack_plus = count.stacks.plus()
        sorter_plus = count.sorters.plus()

        count.list.add(total_tasks=9)
        count.dict.set(key=group_id, total_tasks=10)
        print(f"ID: {group_id}, Stack: {stack_plus}, Sorter: {sorter_plus}", flush=True)

    def worker_minus(count: Counter) -> None:
        for i in range(10):
            # print(f"first key value is {count.dict.get(key=1)}", flush=True)
            next_value = count.list.next()
            print(f"next value is {next_value}", flush=True)
    
    processes = cast(list[mp.Process], [None] * 4)
    for i in range(len(processes)):
        p = mp.Process(target=worker_plus, args=(count,))
        p.start()
        processes[i] = p
    for p in processes: p.join()

    processes = cast(list[mp.Process], [None] * 4)
    for i in range(len(processes)):
        p = mp.Process(target=worker_minus, args=(count,))
        p.start()
        processes[i] = p
    for p in processes: p.join()

    count.unlink()

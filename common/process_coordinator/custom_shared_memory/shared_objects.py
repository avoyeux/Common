"""
To store a shared memory fixed size dictionary.
"""

# IMPORTs standard
import ctypes

# IMPORTs alias
import multiprocessing as mp

# IMPORTs sub
from multiprocessing.shared_memory import SharedMemory

# IMPORTs local
from rustmod import FixedDict, FixedList

# TYPE ANNOTATIONs
from typing import Any

# API public
__all__ = ["SharedValue", "SharedDict", "SharedList"]



class SharedValue:
    """
    A shared memory for storing a single shared value.
    This class has an internal lock and can be safely pickled and unpickled.
    """

    def __init__(self, ctype: type[ctypes._SimpleCData] = ctypes.c_uint32) -> None:
        """
        Initialise a shared memory int.
        This is used to store a shared value that can be accessed and modified by multiple
        processes. The value is protected by a lock to ensure thread safety.

        Args:
            ctype (type[ctypes._SimpleCData], optional): the type of the value to store in the
                shared memory. Defaults to ctypes.c_uint32.

        This class instance can also be safely pickled and unpickled.
        """

        # DATA n LOCK
        self._lock = mp.Lock()
        self._shm = SharedMemory(create=True, size=ctypes.sizeof(ctype))
        self._value = ctype.from_buffer(self._shm.buf)
        self._value.value = 0

        # PICKLE compatibility
        self._shm_name = self._shm.name

    def __getstate__(self) -> dict[str, Any]:
        """
        Returns the state of the object for pickling.
        This is used to ensure that the shared memory name and other necessary attributes are
        preserved when the object is pickled.

        Returns:
            dict[str, Any]: the state of the object.
        """

        state = {
            "lock": self._lock,
            "_shm_name": self._shm_name,
        }
        return state
    
    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        Restores the state of the object from a dictionary.
        This is used to ensure that the shared memory and other necessary attributes are preserved
        when the object is unpickled.

        Args:
            state (dict[str, Any]): the state of the object.
        """

        # SHARED MEMORY attributes
        self._lock = state["lock"]
        self._shm_name = state["_shm_name"]

        # SHARED MEMORY get
        self._shm = SharedMemory(name=self._shm_name)
        self._value = ctypes.c_uint32.from_buffer(self._shm.buf)

    def plus(self, value: int = 1) -> int:
        """
        Adds a value to the shared uint32 and returns the new value.

        Args:
            value (int, optional): the value to add. Defaults to 1.
        
        Returns:
            int: the new value after addition.
        """

        with self._lock: 
            self._value.value += value
            new_value = self._value.value
        return new_value
    
    def minus(self, value: int = 1) -> int:
        """
        Subtracts a value from the shared value and returns the new value.

        Args:
            value (int, optional): the value to subtract. Defaults to 1.

        Returns:
            int: the new value after subtraction.
        """

        with self._lock:
            self._value.value -= value
            new_value = self._value.value
        return new_value

    def close(self) -> None:
        """
        Closing the shared memory.
        """

        self._shm.close()

    def unlink(self) -> None:
        """
        Unlinking the shared memory.
        """

        self._shm.unlink()


class SharedDict:
    """
    A shared memory fixed-sized custom dictionary for storing task metadata.
    The actual hash-table is implemented in Rust for performance.

    The class instance can be safely pickled and unpickled.
    """

    def __init__(self, length: int, nb_of_queues: int) -> None:
        """
        Initialise a shared memory fixed-size dictionary.
        Designed to store the next queue index to use and pops the dictionary key when that key
        should not be used any more.
        The class instance can be safely pickled and unpickled.

        Args:
            length (int): the number of slots in the dictionary.
            nb_of_queues (int): the number of queues to manage.

        Raises:
            ValueError: if the pointer to the shared memory buffer cannot be obtained.
        """

        # DATA n LOCK
        self._lock = mp.Lock()
        self._shm = SharedMemory(create=True, size=FixedDict.total_size(length))
        ptr = ctypes.addressof(ctypes.c_char.from_buffer(self._shm.buf))
        # CHECK pointer
        if ptr is None: raise ValueError("Failed to get pointer from shared memory buffer")
        self._dict = FixedDict(ptr=ptr, capacity=length, nb_of_queues=nb_of_queues)

        # PICKLE compatibility
        self._length = length
        self._nb_of_queues = nb_of_queues
        self._shm_name = self._shm.name

    def __getstate__(self) -> dict[str, Any]:
        """
        Returns the state of the object for pickling.
        This is used to ensure that the shared memory name and other necessary attributes are
        preserved when the object is pickled.

        Returns:
            dict[str, Any]: the state of the object.
        """

        state = {
            '_lock': self._lock,
            '_length': self._length,
            '_nb_of_queues': self._nb_of_queues,
            '_shm_name': self._shm_name,
        }
        return state
    
    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        Restores the state of the object from a dictionary.
        This is used to ensure that the shared memory and other necessary attributes are preserved
        when the object is unpickled.

        Args:
            state (dict[str, Any]): the state of the object.

        Raises:
            ValueError: if the pointer to the shared memory buffer cannot be obtained.
        """

        # SHARED MEMORY attributes
        self._lock = state['_lock']
        self._length = state['_length']
        self._nb_of_queues = state['_nb_of_queues']
        self._shm_name = state['_shm_name']

        # SHARED MEMORY get
        self._shm = SharedMemory(name=self._shm_name)
        ptr = ctypes.addressof(ctypes.c_char.from_buffer(self._shm.buf))
        if ptr is None: raise ValueError("Failed to get pointer from shared memory buffer")
        self._dict = FixedDict(ptr=ptr, capacity=self._length, nb_of_queues=self._nb_of_queues)

    def set(self, key: int, total_tasks: int) -> None:
        """
        Sets a key inside the dictionary.

        Args:
            key (int): the key to set.
            total_tasks (int): the total number of tasks for the key.
        """

        with self._lock: self._dict.set(key, total_tasks)

    def get(self, key: int) -> int:
        """
        Gets a key inside the dictionary.

        Args:
            key (int): the key to get.

        Returns:
            int: the queue index to use for this key.
        """

        with self._lock: return self._dict.get(key)

    def close(self) -> None:
        """
        Closing the shared memory.
        """

        self._shm.close()

    def unlink(self) -> None:
        """
        Unlinking the shared memory.
        """

        self._shm.unlink()


class SharedList:
    """
    A shared memory fixed-size custom list for storing task metadata.
    The actual list is implemented in Rust for performance.

    The class instance can be safely pickled and unpickled.
    """

    def __init__(self, length: int, nb_of_queues: int) -> None:
        """
        Initialise a shared memory fixed-size list.
        Designed to give the next queue index and pops the corresponding value when it should not
        be used any more (i.e. after 'total_tasks' number of times).
        Works as a stack, so 'next()' calls the last value added.
        The class instance can be safely pickled and unpickled.

        Args:
            length (int): the number of slots in the list.
            nb_of_queues (int): the number of queues to manage.

        Raises:
            ValueError: if the pointer to the shared memory buffer cannot be obtained.
        """

        # DATA n LOCK
        self._lock = mp.Lock()
        self._shm = SharedMemory(create=True, size=FixedList.total_size(length))
        ptr = ctypes.addressof(ctypes.c_char.from_buffer(self._shm.buf))
        # CHECK pointer
        if ptr is None: raise ValueError("Failed to get pointer from shared memory buffer")
        self._list = FixedList(ptr=ptr, capacity=length, nb_of_queues=nb_of_queues)

        # PICKLE compatibility
        self._length = length
        self._nb_of_queues = nb_of_queues
        self._shm_name = self._shm.name

    def __getstate__(self) -> dict[str, Any]:
        """
        Returns the state of the object for pickling.
        This is used to ensure that the shared memory name and other necessary attributes are
        preserved when the object is pickled.

        Returns:
            dict[str, Any]: the state of the object.
        """

        state = {
            '_lock': self._lock,
            '_length': self._length,
            '_nb_of_queues': self._nb_of_queues,
            '_shm_name': self._shm_name,
        }
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        Restores the state of the object from a dictionary.
        This is used to ensure that the shared memory and other necessary attributes are preserved
        when the object is unpickled.

        Args:
            state (dict[str, Any]): the state of the object.

        Raises:
            ValueError: if the pointer to the shared memory buffer cannot be obtained.
        """

        # SHARED MEMORY attributes
        self._lock = state['_lock']
        self._length = state['_length']
        self._nb_of_queues = state['_nb_of_queues']
        self._shm_name = state['_shm_name']

        # SHARED MEMORY get
        self._shm = SharedMemory(name=self._shm_name)
        ptr = ctypes.addressof(ctypes.c_char.from_buffer(self._shm.buf))
        if ptr is None: raise ValueError("Failed to get pointer from shared memory buffer")
        self._list = FixedList(ptr=ptr, capacity=self._length, nb_of_queues=self._nb_of_queues)

    def add(self, total_tasks: int) -> None:
        """
        Add a new item to the list.
        Item will be usable 'total_tasks' number of times before being popped out of the list.

        Args:
            total_tasks (int): the total number of tasks associated with the item.
        """

        with self._lock: self._list.add(total_tasks)

    def next(self) -> int:
        """
        Get a queue index from the list.
        Will be the last usable item added to the list.

        Returns:
            int: the queue index to use to get results.
        """

        with self._lock: return self._list.next()

    def close(self) -> None:
        """
        Closing the shared memory.
        """

        self._shm.close()

    def unlink(self) -> None:
        """
        Unlinking the shared memory.
        """

        self._shm.unlink()

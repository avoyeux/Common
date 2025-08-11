"""
Has a class that helps partition tasks across multiple queues.
"""



class IndexAllocator:
    """
    Computes the number of tasks needed for each queue.

    There is one public (staticmethod) function:
        - 'valid_indexes': to get the number of tasks per queue. If no tasks for a certain queue,
            then the corresponding item doesn't exist.
    """

    @staticmethod
    def valid_indexes(number_of_tasks: int, nb_of_queues: int) -> list[int]:
        """
        To get the number of tasks per queue. If there is not enough tasks to fill all the queues,
        then the last queue indexes are empty, hence the len(list[int]) return will be smaller
        than the number of queues.

        Args:
            number_of_tasks (int): the total number of tasks for the same group of tasks.
            nb_of_queues (int): the number of queues to distribute the tasks across.

        Returns:
            list[int]: list of the number of tasks for each queue.
        """

        if number_of_tasks <= 0 or nb_of_queues <= 0: return []
        index_partition = IndexAllocator._partition_integer(total=number_of_tasks, n=nb_of_queues)
        return [n for n in index_partition if n > 0]

    @staticmethod
    def _partition_integer(total: int, n: int) -> list[int]:
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

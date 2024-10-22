"""
To test the MultiProcessing Python file classes.
"""

# Imports
import typing
# Aliases
import numpy as np
import multiprocessing as mp
# Personal 
from common import MultiProcessing, Decorators


class Test:

    def __init__(
            self,          
        ) -> None:

        super().__init__()

        # Attributes

    @Decorators.running_time
    def trying_it_out(self):

        values = [i for i in range(1000)]

        # values = np.array(values)
        # shm, values = MultiProcessing.shared_memory(values)

        # test
        kwargs = {
            'input_data': values,
            'function': self.function,
            'function_kwargs': {
                'random': False,
            },
            'processes': 5,
            'shared_memory_input': False,
            'create_shared_memory': False,
            'transfer_all_data': False,
            'identifier': True,
            'while_True': True,
        }
        results = MultiProcessing.multiprocessing(**kwargs)
        # shm.unlink()
        # print(results)

    @staticmethod
    def function(data: any, index: int | tuple[int, int], random: bool) -> any:

        # print(f'The index value this time is {index}')
        # print(f'The data has type {type(data)}', flush=True)
        return data



if __name__=='__main__':
    
    test = Test()
    test.trying_it_out()
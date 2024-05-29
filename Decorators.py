"""Has decorators that I regularly use.
"""

from functools import wraps
from time import time, ctime
from typeguard import typechecked
from typing import Type, Callable, TypeVar

# General function and decorator types
F = TypeVar('F', bound=Callable[..., any])
D = Callable[[F], any]

@typechecked
def ClassDecorator(decorator: D, functiontype: F | str = 'all') -> Callable[[Type], Type]:
    """Class decorator that applies a given decorator to class functions with the specified function type
    (i.e. classmethod, staticmethod, property, 'regular' or 'instance' -- for an instance method, 
    'all' for all the class functions).

    Args:
        decorator (D): the decorator you want applied to some class functions.
        functiontype (F | str, optional): the specified function type for which you add the decorator. Defaults to 'all'.

    Raises:
        ValueError: if the 'functiontype' is not supported yet, it raises a ValueError.

    Returns:
        Callable[[Type], Type]: returns a function that has the new class with the new decorator applied.
    """

    if functiontype == 'all':
        functiontype = object
    if isinstance(functiontype, str) and (functiontype not in ['regular', 'instance']):
        raise ValueError(f"The string value '{functiontype}' for functiontype is not supported. Choose 'regular', 'instance', or 'all'")

    def Class_rebuilder(cls) -> Type:
        """Rebuilds the class adding the new decorators.

        Returns:
            Type: the new class with the added decorators.
        """

        class NewClass(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        for name, obj in vars(cls).items():
            if callable(obj):
                if not isinstance(functiontype, str):
                    if isinstance(obj, functiontype):
                        method = decorator(obj)
                        setattr(NewClass, name, method)
                elif not isinstance(obj, (staticmethod, classmethod, property)):
                    method = decorator(obj)
                    setattr(NewClass, name, method)
        return NewClass
    return Class_rebuilder
                

class Decorators:
    """To store useful function decorators that I created.
    """
    
    @typechecked
    @staticmethod
    def running_time(func: F) -> F:
        """Gives the starting time (in blue) and ending time (in green) of a given function.
        The name of said function is also printed out.

        Args:
            func (F): the function for which you want to print the running times

        Returns:
            F: the new function with the added decorator.
        """

        @wraps(func)
        def wrapper(*args, **kwargs) -> any:
            """The wrapper that wraps a given function.

            Returns:
                any: returns the result of the function that has just been wrapped.
            """
            START_time = time()
            print(f"\033[94m{func.__name__} started on {ctime(START_time)}. \033[0m")
            result = func(*args, **kwargs)
            END_time = time()
            DIF_time = END_time - START_time
            seconds = DIF_time % 60
            if DIF_time < 60:
                DIF_time = f'{round(seconds, 2)}s'
            elif DIF_time < 3600:
                DIF_time //= 60
                DIF_time = f'{round(DIF_time)}min{round(seconds):02d}s'
            elif DIF_time < 24 * 3600:
                minutes = DIF_time // 60 % 60
                DIF_time //= 3600
                DIF_time = f'{round(DIF_time)}h{round(minutes):02d}min{round(seconds):02d}s'
            else:
                minutes = DIF_time // 60 % 60
                hours = DIF_time // 3600 % 24
                DIF_time //= 24 * 3600
                end_str = 'days' if DIF_time > 1 else 'day'
                DIF_time = f'{round(DIF_time)}{end_str}{hours:02d}h{minutes:02d}min{seconds:02d}s'

            print(f"\033[92m{func.__name__} ended on {ctime(END_time)} ({DIF_time}).\033[0m")
            return result
        return wrapper
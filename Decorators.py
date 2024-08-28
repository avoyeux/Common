#!/usr/bin/env python3.11
"""
Has decorators that I regularly use.
"""

# IMPORTS
from functools import wraps
from time import time, ctime
from typeguard import typechecked
from typing import Type, Callable, TypeVar

# PUBLIC API
__all__ = ['ClassDecorator', 'Decorators']

# General function and decorator types
F = TypeVar('F', bound=Callable[..., any])
D = Callable[[F], any]



@typechecked
def ClassDecorator(decorator: D, functiontype: F | str = 'all') -> Callable[[F], F]:
    """
    Class decorator that applies a given decorator to class functions with the specified function type
    (i.e. classmethod, staticmethod, property, 'regular' or 'instance' -- for an instance method, 
    'all' for all the class functions).

    Args:
        decorator (D): the decorator you want applied to some class functions.
        functiontype (F | str, optional): the specified function type for which you add the decorator. Defaults to 'all'.

    Raises:
        ValueError: if the 'functiontype' is not supported yet, it raises a ValueError.

    Returns:
        Callable[[F], F]: returns a function that has the new class with the new decorator applied.
    """

    if functiontype == 'all':
        functiontype = object
    if isinstance(functiontype, str) and (functiontype not in ['regular', 'instance']):
        raise ValueError(f"The string value '{functiontype}' for functiontype is not supported. Choose 'regular', 'instance', or 'all'")

    def class_rebuilder(cls) -> Type:
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
    return class_rebuilder
                

class Decorators:
    """
    To store useful function decorators that I created.
    """
    
    @staticmethod
    def running_time(verbose_name: str = 'verbose', flush_name: str = 'flush') -> Callable[[F], F]:
        """
        The 'shell' of the Decorators.Utils._actual_running_time_decorator decorator function. This shell was created to decide when a decorator factory needs to
        be used (i.e. when arguments are given or () is used). In the case that no parentheses are used, then the decorator needs to be a simple one and hence using
        a decorator factory would raise major errors. For the documentation of what this decorator/decorator factory does, look at the docstring for the aforementioned
        function.

        Args:
            verbose_name (str, optional): the attribute name that specifies the verbosity in the prints. The attribute == 0 is no prints and > 3 means prints everything.
                Defaults to 'verbose'.
            flush_name (str, optional): the attribute name that specifies if you force the internal buffer to immediately write the output to it's destination,
                i.e. Forces the prints. Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted
                exactly when it is called (usually not the case when multiprocessing). Defaults to 'flush'.

        Returns:
            Callable[[F], F]: returns the decorated function.
        """

        # Checking if running_time is used without (), i.e. the function is passed directly without arguments or an empty ()
        if callable(verbose_name):
            
            # Setting verbose_name as the actual function to be decorated
            func = verbose_name
            verbose_name = 'verbose'
            flush_name = 'flush'
            return DecoratorsUtils._actual_running_time_decorator(func, verbose_name, flush_name)
        
        # If arguments are passed or empty brackets are used, i.e. @Decorators.running_time()
        def decorator(func: F) -> F:
            return DecoratorsUtils._actual_running_time_decorator(func, verbose_name, flush_name)
        return decorator


class DecoratorsUtils:
    """
    Stores private functions that are used to help the added functionalities in this Decorators.py code file.
    """

    @staticmethod
    def _actual_running_time_decorator(func: F, verbose_name: str, flush_name: str) -> F:
        """
        Decorator that prints the starting time of the decorated function in bold blue, and prints the finish running time in bold green.
        The finish time is in readable format, i.e. if any, the time is separated in days, hours, minutes, seconds and centiseconds if the total time is less than a
        minute.
        Furthermore, the printing is decided on the function attributes if the function has the necessary attributes. If not, then the running time is still printed.
        You can also force the internal buffer to immediately write the output to it's destination, i.e. force the prints if the attribute with name 'flush_name' exists.

        Args:
            func (F): the function that needs decorating.
            verbose_name (str): the attribute name of the attribute that sets the verbosity in the prints. The higher this attribute value, the more prints there are.
                The corresponding attribute value does need to be an integer where 0 means no prints at all. If the attribute is not found, then the running time print
                is still done as verbose defaults to 1.
            flush_name (str): the attribute name of the attribute that sets the internal buffer to immediately write the output to it's destination, i.e. it decides to
                force the prints or not. Has a negative effect on the running efficiency as you are forcing the buffer but makes sure that the print is outputted
                exactly when it is called (usually not the case when multiprocessing). If the attribute name is not found, then flush defaults to False.

        Returns:
            F: the decorated function.
        """

        @wraps(func)
        def wrapper(*args: any, **kwargs: any) -> any:
            """
            The usual wrapper for decorators.

            Returns:
                any: the results of the function after running.
            """

            if args:
                class_instance = args[0]
            else:
                class_instance = None
            
            # Checking the function attributes
            class_instance = args[0] if args else None
            verbose = getattr(class_instance, verbose_name, 1) if class_instance is not None else 1
            flush = getattr(class_instance, flush_name, False) if class_instance is not None else False

            # Double checking to make sure that the function doesn't have attributes with the same name but meaning something different.
            verbose = verbose if isinstance(verbose, int) else 1
            flush = flush if isinstance(flush, bool) else False

            START_time = time()
            if verbose > 0: print(f"\033[94m{func.__name__} started on {ctime(START_time)}.\033[0m", flush=flush)

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                print(f"\033[1;31mFunction {func.__name__} didn't run properly. The corresponding error is: {e}")
                raise
            finally:
                END_time = time()
                DIF_time = END_time - START_time

                time_string = DecoratorsUtils._format_time_seconds(DIF_time)
                if verbose > 0: print(f"\033[92m{func.__name__} ended on {ctime(END_time)} ({time_string}).\033[0m", flush=flush)                 
            return result
        return wrapper

    @staticmethod
    def _format_time_seconds(seconds: int | float) -> str:
        """
        Creates as string based on a time input in seconds. The string shows the corresponding time in days, hours, minutes and seconds.

        Args:
            seconds (int | float): the time in seconds that need to be converted in a string human format.

        Returns:
            str: the corresponding time given as a string with the corresponding days, hours, minutes and seconds.
        """

        minutes, seconds = divmod(seconds, 60)
        hours, minutes = map(int, divmod(minutes, 60))
        days, hours = map(int, divmod(hours, 24))

        if minutes == 0:
            return f'{round(seconds, 2)}s'
        elif hours == 0:
            return f'{minutes}m{round(seconds):02d}s'
        elif days == 0:
            return f'{hours}h{minutes:02d}m{round(seconds):02d}s'
        else:
            return f'{days}d{hours:02d}h{minutes:02d}m{round(seconds):02d}s'
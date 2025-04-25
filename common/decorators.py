#!/usr/bin/env python3.11
"""
Has decorators that I regularly use.
"""

# IMPORTs
import inspect
import functools

# IMPORTs alias
import numpy as np

# IMPORTs sub
from time import time, ctime
from typeguard import typechecked

# PUBLIC API
__all__ = ['ClassDecorator', 'Decorators']

# TYPE ANNOTATIONs
from typing import Any, cast, Type, Callable, TypeVar, overload, Literal
F = TypeVar('F', bound=Callable[..., Any])
D = Callable[[F], any]
T = TypeVar('T', bound=Type)



@typechecked
def ClassDecorator(
        decorator: D,
        functiontype: T | Literal['regular', 'instance', 'all'] = 'all',
    ) -> Callable[[T], T]:
    """
    Class decorator that applies a given decorator to class functions with the specified function
    type (i.e. classmethod, staticmethod, property, 'regular' or 'instance' -- for an instance
    method, 'all' for all the class functions).

    Args:
        decorator (D): the decorator you want applied to some class functions.
        functiontype (F | str, optional): the specified function type for which you add the
            decorator. Defaults to 'all'.

    Raises:
        ValueError: if the 'functiontype' is not supported yet, it raises a ValueError.

    Returns:
        Callable[[F], F]: returns a function that has the new class with the new decorator applied.
    """

    if functiontype == 'all': functiontype = object
    if isinstance(functiontype, str) and (functiontype not in ['regular', 'instance']):
        raise ValueError(
            f"The string value '{functiontype}' for functiontype is not supported. "
            "Choose 'regular', 'instance', or 'all'"
        )

    def class_rebuilder(cls: T) -> T:
        """
        Rebuilds the class adding the new decorators.

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
    # todo add decorator that prints all inputs.

    @overload
    @staticmethod
    def running_time(func: F) -> F: ...

    @overload
    @staticmethod
    def running_time(*, verbose_name: str = ..., flush_name: str = ...) -> Callable[[F], F]: ...

    @overload  # fallback
    @staticmethod
    def running_time(
            func: F | None = None,
            *,
            verbose_name: str = ...,
            flush_name: str = ...,
        ) -> F | Callable[[F], F]: ...

    @staticmethod  
    def running_time(
            func: F | None = None,
            *,
            verbose_name: str = 'verbose',
            flush_name: str = 'flush',
        ) -> F | Callable[[F], F]:
        """
        The 'shell' of the Decorators.Utils._actual_running_time_decorator decorator function. This
        shell was created to decide when a decorator factory needs to be used (i.e. when arguments
        are given or () is used). In the case that no parentheses are used, then the decorator
        needs to be a simple one and hence using a decorator factory would raise major errors. For
        the documentation of what this decorator/decorator factory does, look at the docstring for
        the aforementioned function.

        Args:
            verbose_name (str, optional): the attribute name that specifies the verbosity in the
                prints. The attribute == 0 is no prints and > 3 means prints everything.
                Defaults to 'verbose'.
            flush_name (str, optional): the attribute name that specifies if you force the internal
                buffer to immediately write the output to it's destination, i.e. Forces the prints.
                Has a negative effect on the running efficiency as you are forcing the buffer but
                makes sure that the print is outputted exactly when it is called (usually not the
                case when multiprocessing). Defaults to 'flush'.

        Returns:
            Callable[[F], F]: returns the decorated function.
        """

        # CHECK running_time used without (): (function passed directly without ())
        if func is not None and callable(func):
            return DecoratorsUtils.actual_running_time_decorator(func, verbose_name, flush_name)
        
        # If arguments are passed or empty brackets are used, i.e. @Decorators.running_time()
        def decorator(func: F) -> F:
            return DecoratorsUtils.actual_running_time_decorator(func, verbose_name, flush_name)
        return decorator

    @staticmethod
    def print_inputs(
            defaults: F | bool = False,
            type_: bool = True,
            value: bool = False,
            shape: bool = True,
            size: bool = False,
            flush: bool = False,
        ) -> Callable[[F], F]:
        """  #TODO: this shouldn't yet be finished. Needs testing from what I remember.
        To print the argument names and information of a decorated function. You can also choose
        what should be printed.

        Args:
            defaults (F | bool, optional): When decorator arguments are given, it lets you decide
                if the default arguments that weren't called should be printed. If no arguments are
                set, it points to the decorated function just like a decorator would.
                Defaults to False.
            type_ (bool, optional): deciding to print the type of each argument. Defaults to True.
            value (bool, optional): deciding to print the value of the argument. Defaults to False.
            shape (bool, optional): deciding to print the shape of the argument. It outputs len() 
                if the argument isn't a dict or a ndarray. Defaults to True.
            size (bool, optional): deciding to print the size of the argument. Only work if the
                argument is a ndarray. Defaults to False.
            flush (bool, optional): deciding to force the internal buffer to immediately write the
                output to it's destination, i.e. it decides to force the prints or not. Has a
                negative effect on the running efficiency as you are forcing the buffer but makes
                sure that the print is outputted exactly when it is called (usually not the case
                when multiprocessing). Defaults to False.

        Returns:
            Callable[[F], F]: returns the decorated function
        """

        if callable(defaults):
            func = defaults
            result = DecoratorsUtils.actual_print_inputs_decorator(
                func=func,
                defaults=False,
                type_=type_,
                value=value,
                shape=shape,
                size=size,
                flush=flush,
            )
            return result

        def decorator(func: F) -> F:
            result = DecoratorsUtils.actual_print_inputs_decorator(
                func=func,
                defaults=defaults,
                type_=type_,
                value=value,
                shape=shape,
                size=size,
                flush=flush,
            )
            return result
        return decorator


class DecoratorsUtils:
    """
    Stores private functions that are used to help the added functionalities in this Decorators
    class.
    """

    @staticmethod
    def actual_running_time_decorator(func: F, verbose_name: str, flush_name: str) -> F:
        """
        Decorator that prints the starting time of the decorated function in bold blue, and prints
        the finish running time in bold green. The finish time is in readable format, i.e. if any,
        the time is separated in days, hours, minutes, seconds and centiseconds if the total time
        is less than a minute. Furthermore, the printing is decided on the function attributes if
        the function has the necessary attributes. If not, then the running time is still printed.
        You can also force the internal buffer to immediately write the output to it's destination,
        i.e. force the prints if the attribute with name 'flush_name' exists.

        Args:
            func (F): the function that needs decorating.
            verbose_name (str): the attribute name of the attribute that sets the verbosity in the
                prints. The higher this attribute value, the more prints there are. The
                corresponding attribute value does need to be an integer where 0 means no prints at
                all. If the attribute is not found, then the running time print is still done as
                verbose defaults to 1.
            flush_name (str): the attribute name of the attribute that sets the internal buffer to
                immediately write the output to it's destination, i.e. it decides to force the
                prints or not. Has a negative effect on the running efficiency as you are forcing
                the buffer but makes sure that the print is outputted exactly when it is called
                (usually not the case when multiprocessing). If the attribute name is not found,
                then flush defaults to False.

        Returns:
            F: the decorated function.
        """

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            The usual wrapper for decorators.

            Raises:
                Exception: when the decorated function didn't run.

            Returns:
                Any: the results of the function after running.
            """
            
            # CHECK function attributes
            class_instance = args[0] if args else None
            verbose = getattr(class_instance, verbose_name, 1) if class_instance is not None else 1
            flush = getattr(
                class_instance, flush_name, False
            ) if class_instance is not None else False

            # CHECK right attribute is chosen
            verbose = verbose if isinstance(verbose, int) else 1
            flush = flush if isinstance(flush, bool) else False

            START_time = time()
            if verbose > 0: 
                print(
                    f"\033[94m{func.__qualname__} started on {ctime(START_time)}.\033[0m",
                    flush=flush,
                )

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                raise Exception(
                    f"\033[1;31mFunction {func.__qualname__} didn't run properly."
                    f"The corresponding error is: {e}\033[0m"
                )
            finally:
                END_time = time()
                DIF_time = END_time - START_time

                time_string = DecoratorsUtils._format_time_seconds(DIF_time)
                if verbose > 0:
                    print(
                        f"\033[92m{func.__qualname__} ended on {ctime(END_time)} ({time_string})."
                        "\033[0m",
                        flush=flush,
                    )                 
            return result
        return cast(F, wrapper)

    @staticmethod
    def _format_time_seconds(seconds: int | float) -> str:
        """
        Creates as string based on a time input in seconds. The string shows the corresponding time
        in days, hours, minutes and seconds.

        Args:
            seconds (int | float): the time in seconds that need to be converted in a string human
                format.

        Returns:
            str: the corresponding time given as a string with the corresponding days, hours,
                minutes and seconds.
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
        
    @staticmethod
    def actual_print_inputs_decorator(
            func: F,
            defaults: bool,
            type_: bool,
            value: bool,
            shape: bool,
            size: bool,
            flush: bool,
        ) -> F:
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            #TODO: copy the docstring for the other wrapper.

            # Get signature
            signature = inspect.signature(func)

            # Bind args
            bound_args = signature.bind(*args, **kwargs)
            if defaults: bound_args.apply_defaults()

            print(f'\033[1;0m{func.__qualname__}\033[1;35m arguments are:')
            for name, item in bound_args.arguments.items():
                DecoratorsUtils._printing_options(name, item, type_, value, shape, size, flush)
            return func(*args, **kwargs)
        return cast(F, wrapper)

    @staticmethod
    def _printing_options(
            name: str,
            item: Any,
            type_: bool,
            value: bool,
            shape: bool,
            size: bool,
            flush: bool,
        ) -> None:
        # TODO: to print the args info

        print_str = f'\033[0;35m  {name} -- '

        # Choices
        if type_: print_str += f'type: {type(item)}, '
        if value: print_str += f'value: {item}, '
        if isinstance(item, np.ndarray): 
            if shape: print_str += f'shape: {item.shape}, '
            if size: print_str += f'size: {item.size}, '
        elif isinstance(item, list):
            if shape: print_str +=f'len: {len(item)}, '

        # Result
        print(print_str.rstrip(', ') + '.\033[0m', flush=flush)

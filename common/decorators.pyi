"""
Only for the extensive type hints so that the main code doesn't become less readable.
"""

# IMPORTs

# IMPORTs sub
from typing import Any, overload, final, TypeVar, Callable

# General function and decorator types
F = TypeVar('F', bound=Callable[..., Any])

@final
class Decorators:

    @overload
    @staticmethod
    def running_time(func: F) -> F: ...

    @overload
    @staticmethod
    def running_time(*, verbose_name: str = ..., flush_name: str = ...) -> Callable[[F], F]: ...

    @overload
    @staticmethod
    def running_time(
            func: F | None = None,
            *,
            verbose_name: str = 'verbose',
            flush_name: str = 'flush',
        ) -> F | Callable[[F], F]: ...

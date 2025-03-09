"""
Only to show the extensive type hints so that the main code doesn't become less readable.
"""

# IMPORTs sub
from typing import final, overload


@final
class SSHMirroredFilesystem:

    @overload
    def mirror(self, remote_filepaths: str, strip_level: int = ...) -> str: ...

    @overload
    def mirror(self, remote_filepaths: list[str], strip_level: int = ...) -> list[str]: ...

    @overload
    def mirror(
            self,
            remote_filepaths: str | list[str],
            strip_level: int = ...,
        ) -> str | list[str]: ...
    
    @overload
    @staticmethod
    def remote_to_local(
            remote_filepaths: str,
            host_shortcut: str = ...,
            compression: str = ...,
            strip_level: int = ...,
        ) -> str: ...

    @overload
    @staticmethod
    def remote_to_local(
            remote_filepaths: list[str],
            host_shortcut: str = ...,
            compression: str = ...,
            strip_level: int = ...,
        ) -> list[str]: ...

    @overload
    @staticmethod
    def remote_to_local(
            remote_filepaths: str | list[str],
            host_shortcut: str = ...,
            compression: str = ...,
            strip_level: int = ...,
        ) -> str | list[str]: ...

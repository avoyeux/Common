"""
Has functions to help with pandas operations.
"""

# TYPE ANNOTATIONs
from typing import TypeVar
T = TypeVar('T')

# API public
__all__ = ["Pandas"]



class Pandas:
    """
    To help complete some pandas dataframe manipulations.
    """
    
    @staticmethod
    def safe_round(
            x: T,
            decimals: int = 0,
            try_convert_string: bool = False,
            verbose: int = 0,
            flush: bool = False,
        ) -> T:
        """
        To round values if possible. You can also choose to round strings if it represents a 
        number.

        Args:
            x (Any): value to be rounded if possible.
            decimals (int, optional): number of decimals for the output. Defaults to 0.
            try_convert_strings (bool, optional): to convert and round strings when possible.
                Defaults to False.
            verbose (int, optional): Choosing to print the error if verbose > 0. Defaults to 0.
            flush (bool, optional): sets the internal buffer to immediately write the output to
                it's destination, i.e. it decides to force the prints or not. Has a negative effect
                on the running efficiency as you are forcing the buffer but makes sure that the
                print is outputted exactly when it is called (usually not the case when
                multiprocessing). Defaults to False.

        Returns:
            Any: the rounded value or the input value if rounding not possible.
        """

        try:
            if try_convert_string: x = float(x)  #type:ignore
            return round(x, decimals)  #type:ignore
        except Exception as e:
            if verbose > 0:
                print(
                    f"\033[31mPandas.safe_round exception occurred: {e}. "
                    "Keeping initial value.\033[0m",
                    flush=flush,
                )
            return x

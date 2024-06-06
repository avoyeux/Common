"""Has functions to help with pandas operations.
"""

# Imports 
from typeguard import typechecked

class Pandas:
    """To help complete some pandas dataframe manipulations.
    """
    
    @typechecked
    @staticmethod
    def safe_round(x: any, decimals: int = 0, try_convert_string: bool = False, verbose: int = 0) -> any:
        """To round values if possible. You can also choose to round strings if it represents a number.

        Args:
            x (any): value to be rounded if possible.
            decimals (int, optional): number of decimals for the output. Defaults to 0.
            try_convert_strings (bool, optional): to convert and round strings when possible. Defaults to False.
            verbose (int, optional): Choosing to print the error if verbose > 0. Defaults to 0.

        Returns:
            any: the rounded value or the input value if rounding not possible.
        """
        try:
            if try_convert_string: x = float(x)
            return round(x, decimals)
        except Exception as e:
            if verbose > 0: print(f'\033[31mPandas.safe_round exception occured: {e}. Keeping initial value.\033[0m')
            return x
        
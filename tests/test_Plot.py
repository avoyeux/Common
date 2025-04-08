"""
To test the Plot.py functions
"""

# IMPORTs
import os

# IMPORTs alias
import numpy as np

# IMPORTs sub
from typing import cast
import matplotlib.pyplot as plt

# IMPORTs personal
from Common.common import Decorators, Plot, AnnotateAlongCurve

# PATH setup
results_dump_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results_dump')



class TestColours:
    """
    Tests if the colour names are compatible with the plt.scatter() function.
    """

    @Decorators.running_time
    def __init__(self, omit: list[str]) -> None:
        """
        To test the colours by plotting a scatter plot with the different colours.

        Args:
            omit (str | list[str]): the colour to be omitted from the test.
        """

        # Setup
        self.generator = Plot.different_colours(omit=omit)
        self.first_colour = next(self.generator)
        self.x = np.array([0], dtype='int8')

        # Run
        self.main(self.first_colour)

    def main(self, colour: str) -> None:
        """
        Testing the colour choices to see if they are compatible with plt.scatter().

        Args:
            colour (str): the colour to be tested. 
        """

        while True:
            try:
                plt.scatter(self.x, self.x, color=colour)
                print(colour)
            except Exception as e:
                print(f"The plt.scatter didn't work. Error is: {e}")
                print(f'If you want to continue with the colour tries input 1, else 0.')
                choice = int(input('Input: '))
                if not choice: return 
            try:
                colour = next(self.generator)
            except StopIteration:
                print('All colours tested.')
                return


class TestContours:
    """
    To run visual tests of the contours function.
    This class just creates a random mask and plots the mask with the corresponding contours.
    """

    @Decorators.running_time
    def __init__(self) -> None:
        """
        To run a test of the contours by creating the contours for a random mask.
        """

        # RUN
        self.mask = self.create_mask()
        self.plot()

    def create_mask(self) -> np.ndarray:
        """
        To create a mask for testing the contours function.

        Returns:
            np.ndarray: the testing mask.
        """

        arr = np.random.rand(100, 200)
        filters = arr > 0.8

        mask = np.zeros(arr.shape)
        mask[filters] = 1
        return mask

    def plot(self) -> None:
        """
        To plot the contours of the mask.
        """
        
        # Get contours
        lines = Plot.contours(self.mask)

        plt.figure(figsize=(12, 5))

        first_lines = lines[0]
        plt.plot(
            first_lines[1], 
            first_lines[0],
            color='r',
            linewidth=1,
            label='contour',
        )
        for line in lines[1:]:
            plt.plot(
                line[1],
                line[0],
                color='r',
                linewidth=1,
            )
        
        # Plot mask
        plt.title('Testing contours result')
        plt.imshow(self.mask, alpha=0.4, label='mask')
        plt.legend()
        plt.savefig(os.path.join(results_dump_path, 'test_plot_contours_image.png'), dpi=500)
        plt.close()
        print('Test image with contours saved.')


class TestAnnotations:

    @Decorators.running_time
    def __init__(self, a: int | float, b: int | float, c: int | float) -> None:

        # ATTRIBUTEs
        self.a = a
        self.b = b
        self.c = c

        # RUN
        x, y, t = self.create_data()
        self.create_figure(x, y)
        self.add_annotations(x, y, t)
    
    def create_data(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:

        # COORDs curve
        x = np.arange(100, dtype='float64')
        y = self.a * x**3 - self.b * x + self.c

        # CUMULATIVE arc length
        arc_length = np.empty(x.size, dtype='float64')
        arc_length[0] = 0
        for i in range(1, x.size):
            dx = x[i] - x[i - 1]
            dy = y[i] - y[i - 1]
            arc_length[i] = arc_length[i - 1] + np.sqrt(dx**2 + dy**2)
        arc_length /= arc_length[-1]  # normalise
        return x, y, arc_length

    # def nth_order_polynomial(self, t: np.ndarray, *coeffs: int | float) -> np.ndarray:
    #     """
    #     Polynomial function given a 1D ndarray and the polynomial coefficients. The polynomial
    #     order is defined before hand.

    #     Args:
    #         t (np.ndarray): the 1D array for which you want the polynomial results.
    #         coeffs (int | float): the coefficient(s) for the polynomial in the order a_0 + 
    #             a_1 * t + a_2 * t**2 + ...

    #     Returns:
    #         np.ndarray: the polynomial results.
    #     """

    #     # INIT
    #     result: np.ndarray = cast(np.ndarray, 0)

    #     # POLYNOMIAL
    #     for i in range(3): result += coeffs[i] * t ** i
    #     return result

    def create_figure(self, x, y) -> None:

        plt.figure(figsize=(12, 5))
        plt.title('Testing annotations')

        plt.plot(x, y, color='b', linewidth=1, label='curve')

        plt.xlabel('x')
        plt.ylabel('y')

    def add_annotations(self, x, y, t) -> None:

        # ANNOTATE curve
        step = 0.3
        offset = 0
        AnnotateAlongCurve(x, y, t, step=step, offset=offset)

        plt.legend()
        plt.savefig(os.path.join(results_dump_path, 'test_plot_annotations_image.png'), dpi=500)
        plt.close()
        print('Test image with annotations saved.')



if __name__ == '__main__':

    # TestColours(omit='')
    # TestContours()
    TestAnnotations(-0.5, -2, 4)

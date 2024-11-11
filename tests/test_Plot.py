"""
To test the Plot.py functions
"""

# Imports
import numpy as np
import matplotlib.pyplot as plt
# Personal imports
from common import Plot, Decorators



class TestColours:

    @Decorators.running_time
    def __init__(self, omit: str | list[str]):

        # Setup
        self.generator = Plot.different_colours(omit=omit)
        self.first_colour = next(self.generator)
        self.x = np.array([0], dtype='int8')

        # Run
        self.main(self.first_colour)

    def main(self, colour: str):
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

if __name__ == '__main__':
    TestColours(omit='')
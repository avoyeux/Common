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


class TestContours:

    @Decorators.running_time
    def __init__(self):

        self.mask = self.create_mask()

        self.plot()

    def create_mask(self) -> np.ndarray:

        arr = np.random.rand(100, 200)
        filters = arr > 0.5

        mask = np.zeros(arr.shape)
        mask[filters] = 1
        return mask

    def plot(self):
        
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
        plt.imshow(self.mask, alpha=0.4, label='mask', origin='lower')
        plt.legend()
        plt.savefig('test_plot_contours_image.png', dpi=500)
        plt.close()
        print('Test image with contours saved.')





if __name__ == '__main__':
    # TestColours(omit='')

    TestContours()
"""
To store function used for common formatting tasks.
"""

# IMPORTS
import re



class stringFormatter:
    """
    To reformat strings so that they are of a set length.
    """

    def __init__(
            self,
            max_length: int,
            indentation: str = '| ',
            ansi: bool = False,
        ) -> None:

        self.max_length = max_length
        self.indentation = indentation
        self.ansi = ansi

        # RUN-TIME 
        self.new_line_pattern = re.compile(r'(\n*)') 
        self.ansi_escape_pattern = re.compile(r'(\033\[[0-9;]*m)')

    def reformat_string(
            self,
            string: str,
            prefix: str = '',
            suffix: str = '',
            rank: int = 0,
        ) -> list[str]:
        """
        To reformat a given string so that you can set a maximum length for each string line (i.e. 
        before each linebreak). You can also choose the add a prefix and/or a suffix to the string
        and also have ANSI code in the inputs by setting 'ansi_all'=True. Slower that way, hence 
        the default being 'ansi_all'=False.

        Args:
            string (str): the string to reformat.
            prefix (str, optional): the string put at the beginning of the result. Defaults to ''.
            suffix (str, optional): the string put at the end of the result. Defaults to ''.
            rank (int, optional): the rank in the hierarchy. The higher the rank, the further down
                in the file hierarchy the printed information is. It is to decide how many times
                the indentation has to be added at the beginning of each string in the outputted
                list. Defaults to 0.

        Returns:
            list[str]: the list of reformatted strings with each having a maximum visualised length
                (i.e. when printed on a terminal that recognises ANSI code). When printing the 
                strings, there should be a linebreak between each list elements.
        """

        # CHECK init
        if len(string)==0: return ['']

        # SETUP init
        string = prefix + string + suffix
        max_string_len = self.max_length - len(self.indentation) * rank

        # If ANSI everywhere
        if self.ansi:
            description = []
            for section in string.split('\n'):
                current_segment = []
                current_len = 0

                for piece in self.ansi_escape_pattern.split(section):
                    if self.ansi_escape_pattern.match(piece):
                        current_segment.append(piece)
                    else:
                        while len(piece) > 0:
                            space_left = max_string_len - current_len

                            if len(piece) <= space_left:
                                current_segment.append(piece)
                                current_len += len(piece)
                                piece = ''
                            else:
                                current_segment.append(piece[:space_left])
                                description.append(''.join(current_segment))
                                current_segment = []
                                current_len = 0
                                piece = piece[space_left:]
                description.append(''.join(current_segment))
        else:
            description = [
                section[i:i + max_string_len]
                for section in string.split('\n')  # keeping the desired linebreaks
                for i in range(0, len(section) if len(section)!=0 else 1, max_string_len)
            ]
        description = [
            self.indentation * rank + desc.strip()
            for desc in description
        ]
        return description
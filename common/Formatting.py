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
        self.ansi_escape_pattern = re.compile(r'(\033\[[0-9;]*m)')

    def reformat_string(self, string: str, title: str = '', rank: int = 0) -> list[str]:
        """
        To reformat a given string so that you can set a maximum length for each string line (i.e. before each linebreak).
        You can also choose the add the beginning of the string that usually has ANSI code (as it is the title for the print) or have ANSI code in the string by
        setting 'ansi_all'=True. Slower that way, hence the default being 'ansi_all'=False.

        Args:
            string (str): the string to reformat, with or without the title (usually containing ANSI code).
            title (str, optional): the beginning of the string usually with ANSI code. Defaults to ''.
            rank (int, optional): the rank in the hierarchy. The higher the rank, the further down in the file hierarchy the printed information is.
                It is used to also take into account the indentation length. Defaults to 0.

        Returns:
            list[str]: the list of reformatted strings with each having a maximum visualised length (i.e. when printed on a terminal that recognises ANSI code).
            When printing the strings, there should be a linebreak between each list elements.
        """

        # Setup init
        max_string_len = self.max_length - len(self.indentation) * rank

        # If ANSI everywhere
        if self.ansi:
            string = title + string

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
                                piece = piece[space_left:]
                                current_segment = []
                                current_len = 0
            if current_segment or current_len > 0:
                description.append(''.join(current_segment))
        else:
            # Placeholder for ansi beginning string
            without_ansi = re.sub(self.ansi_escape_pattern, '', title)
            string = len(without_ansi) * ' ' + string 

            description = [
                section[i:i + max_string_len].strip()
                for section in string.split('\n')  # keeping the desired linebreaks
                for i in range(0, len(section), max_string_len)
            ]
            description[0] = title + description[0]
        description = [
            self.indentation * rank + desc
            for desc in description
        ]
        return description
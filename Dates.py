"""
To store code related to date formatting.
"""


class CustomDate:
    """
    To separate the year, month, day, hour, minute, second if a string dateutil.parser.parser doesn't work. 
    """

    def __init__(self, date_str: str | bytes):
        """
        Instance to get .year, .month, ... to .second attributes for a given string or bytestring date in the format
        YYYY-MM-DDThh-mm-ss (str) or YYYY/MM/DD hh:mm:ss (bytes).

        Args:
            date_str (str | bytes): the date for which you want the different information as attributes.
        """
        self.year: int
        self.month: int
        self.day: int
        self.hour: int
        self.minute: int
        self.second: int

        if isinstance(date_str, str):
            self.parse_date_str(date_str=date_str)
        elif isinstance(date_str, bytes):
            self.parse_date_bytes(date_str=date_str)

    def parse_date_str(self, date_str: str) -> None:
        """
        Separating a string in the format YYYY-MM-DDThh-mm-ss to get the different time attributes.

        Args:
            date_str (str): the date string for which you want to get .year, .month and etc.
        """

        date_part, time_part = date_str.split("T")
        self.year, self.month, self.day = map(int, date_part.split("-"))
        self.hour, self.minute, self.second = map(int, time_part.split("-"))
    
    def parse_date_bytes(self, date_str: bytes) -> None:
        """
        Separating a bytestring in the format YYYY/MM/DD hh:mm:ss to get the different date attributes.

        Args:
            date_str (bytes): the date bytestring for which you want to get .year, .month and etc.
        """

        date_part, time_part = date_str.split(b' ')
        self.year, self.month, self.day = map(int, date_part.split(b"/"))
        self.hour, self.minute, self.second = map(int, time_part.split(b':'))
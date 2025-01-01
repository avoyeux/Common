"""
To store code related to date formatting.
"""


class CustomDate:
    """
    To separate the year, month, day, hour, minute, second if a string dateutil.parser.parser
    doesn't work. 
    """

    def __init__(self, date_str: str | bytes) -> None:
        """
        Instance to get .year, .month, ... to .second attributes for a given string or bytestring
        date in the format YYYY-MM-DDThh-mm-ss (str) or YYYY/MM/DD hh:mm:ss (bytes).

        Args:
            date_str (str | bytes): the date for which you want the different information as
                attributes.
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
        Separating a bytestring in the format YYYY/MM/DD hh:mm:ss to get the different date
        attributes.

        Args:
            date_str (bytes): the date bytestring for which you want to get .year, .month and etc.
        """

        date_part, time_part = date_str.split(b' ')
        self.year, self.month, self.day = map(int, date_part.split(b"/"))
        self.hour, self.minute, self.second = map(int, time_part.split(b':'))


class DatesUtils:
    """
    To store staticmethod function to help with dating stuff.
    """

    @staticmethod
    def days_per_month(year: int) -> list[int]:
        """
        To get how many days are in each month for a specific year (as there are leap years).
        For ease of use, the indexing values are the same than the corresponding month number, 
        i.e. index 2 will give the number of days in February. Index 0 just outputs 0.

        Args:
            year (int): the Gregorian calendar year.

        Returns:
            list[int]: list of the days per month with index 0 giving 0, index 1 the days in
                January, etc.
        """

        # Usual days
        days_per_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        # Leap years
        if (year % 4 == 0 and year % 100 !=0) or (year % 400 == 0): days_per_month[2] = 29
        return days_per_month

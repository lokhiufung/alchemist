from functools import total_ordering
from alchemist.enums import ResolutionEnum


@total_ordering
class Frequency:
    resolution_abbreviations_mapping = {
        't': 'TICK',
        'q': 'QUOTE',
        's': 'SECOND',
        'm': 'MINUTE',
        'h': 'HOUR',
        'd': 'DAY'
    }
    resolution_order = ['t', 'q', 's', 'm', 'h', 'd']  # Define the hierarchy of resolutions

    def __init__(self, freq: str):
        """
        Initialize the Frequency object by parsing and validating the input frequency string.

        Args:
            freq (str): A string representing frequency, e.g., "1m", "5h".
        
        Raises:
            ValueError: If the frequency format is invalid or not supported.
        """
        self.freq = freq
        self.unit, self.resolution = self._parse_freq(freq)
        self.resolution = self.resolution.lower()
        self.normalized_value = self.get_normalized_value()

        if self.resolution not in self.resolution_abbreviations_mapping:
            raise ValueError(
                f"Invalid resolution: {self.resolution}. Allowed resolutions are: {list(self.resolution_abbreviations_mapping.keys())}"
            )

        if self.unit <= 0:
            raise ValueError(f"Invalid unit: {self.unit}. Unit must be a positive integer.")

        # Validate resolution with ResolutionEnum
        ResolutionEnum.validate(self.resolution_abbreviations_mapping[self.resolution])

    def get_normalized_value(self):
        """
        Calculate and return the normalized duration of the frequency in seconds.

        **Bar Indexing Convention**:
        - **Start Convention**: The index represents the start of the bar's time window.
          For example, the bar indexed at `09:30:00` covers the interval from `09:30:00` to `09:30:59`.
        - **End Convention**: The index represents the end of the bar's time window.
          For example, the bar indexed at `09:30:00` covers the interval from `09:29:00` to `09:29:59`.

        **Implementation Note**:
        - For resampling to larger timeframes, such as `5m`, the method accounts for
          the range of `0-4` minutes within a 5-minute bar.
        - The "+1" adjustment ensures that the transition to a new bar is correctly detected.
        - **Unit Divisor Restriction**: Only unit divisors of 10 are allowed (e.g., 1, 2, 5, 10).
          Frequencies with units not divisible by 10 will raise a `ValueError`.

        **Supported Resolutions**:
        - 's' (SECOND)
        - 'm' (MINUTE)
        - 'h' (HOUR)
        - 'd' (DAY)

        **Raises**:
            ValueError: If the resolution is not time-based ('s', 'm', 'h', 'd') or if the unit is not a divisor of 10.

        Returns:
            int: The total number of seconds represented by the frequency.
        """
        if self.resolution == 's':
            # Unit divisor check
            if self.unit not in [1, 2, 5, 10]:
                raise ValueError(f"Invalid unit: {self.unit}. Only unit divisors of 10 are allowed for 's' resolution.")
            return self.unit
        elif self.resolution == 'm':
            if self.unit not in [1, 2, 5, 10]:
                raise ValueError(f"Invalid unit: {self.unit}. Only unit divisors of 10 are allowed for 'm' resolution.")
            return self.unit * 60
        elif self.resolution == 'h':
            if self.unit not in [1, 2, 5, 10]:
                raise ValueError(f"Invalid unit: {self.unit}. Only unit divisors of 10 are allowed for 'h' resolution.")
            return self.unit * 3600
        elif self.resolution == 'd':
            if self.unit not in [1, 2, 5, 10]:
                raise ValueError(f"Invalid unit: {self.unit}. Only unit divisors of 10 are allowed for 'd' resolution.")
            return self.unit * 86400
        else:
            return None

    @staticmethod
    def _parse_freq(freq: str):
        """
        Parse the frequency string into unit and resolution.

        Args:
            freq (str): A string representing frequency, e.g., "1m", "5h".
        
        Returns:
            tuple: (unit: int, resolution: str)

        Raises:
            ValueError: If the frequency format is invalid.
        """
        try:
            unit = int(freq[:-1])  # Extract the numeric part
            resolution = freq[-1]  # Extract the last character as resolution
            return unit, resolution
        except (ValueError, IndexError):
            raise ValueError(f"Invalid frequency format: {freq}. Expected format like '1m', '5h', etc.")

    def _get_resolution_order(self, resolution):
        return self.resolution_order.index(resolution)
    
    def __eq__(self, frequency):
        """
        Check equality between two Frequency objects.
        """
        if not isinstance(frequency, Frequency):
            return NotImplemented
        return self.unit == frequency.unit and self.resolution == frequency.resolution

    def __lt__(self, frequency):
        """
        Compare if this frequency is less than another, handling the `quote` (q) case.
        """
        if not isinstance(frequency, Frequency):
            return NotImplemented

        # Special rule for `quote` (q)
        if 'q' in (self.resolution, frequency.resolution):
            if self.resolution == 'q' and frequency.resolution != 'q':
                raise ValueError("Cannot compare `quote` frequency (`q`) with other resolutions.")
            if frequency.resolution == 'q' and self.resolution != 'q':
                raise ValueError("Cannot compare `quote` frequency (`q`) with other resolutions.")

        # Compare resolutions first
        if self.resolution != frequency.resolution:
            return self._get_resolution_order(self.resolution) < self._get_resolution_order(frequency.resolution)

        # If resolutions are the same, compare units
        return self.unit < frequency.unit

    def __str__(self):
        """
        String representation of the Frequency object.

        Returns:
            str: Human-readable frequency description.
        """
        full_name = self.resolution_abbreviations_mapping[self.resolution]
        return f"{self.unit}{self.resolution.upper()} ({full_name})"

    def __repr__(self):
        return f"Frequency(freq='{self.freq}')"

    @classmethod
    def get_supported_resolutions(cls):
        """
        Get a list of supported resolutions.

        Returns:
            list: Supported resolution keys.
        """
        return list(cls.resolution_abbreviations_mapping.keys())
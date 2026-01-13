"""
Datetime functions for Sparkless.

This module provides comprehensive datetime functions that match PySpark's
datetime function API. Includes date/time conversion, extraction, and manipulation
operations for temporal data processing in DataFrames.

Key Features:
    - Complete PySpark datetime function API compatibility
    - Current date/time functions (current_timestamp, current_date)
    - Date conversion (to_date, to_timestamp)
    - Date extraction (year, month, day, hour, minute, second)
    - Date manipulation (dayofweek, dayofyear, weekofyear, quarter)
    - Type-safe operations with proper return types
    - Support for various date formats and time zones
    - Proper handling of date parsing and validation

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"timestamp": "2024-01-15 10:30:00", "date_str": "2024-01-15"}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.year(F.col("timestamp")),
    ...     F.month(F.col("timestamp")),
    ...     F.to_date(F.col("date_str"))
    ... ).show()
    DataFrame[1 rows, 3 columns]
    year(timestamp) month(timestamp) to_date(date_str)
    2024 1 2024-01-15
"""

from typing import Optional, Union
from sparkless.functions.base import Column, ColumnOperation
from sparkless.functions.core.literals import Literal
from sparkless.core.type_utils import normalize_date_input, get_expression_name


class DateTimeFunctions:
    """Collection of datetime functions."""

    @staticmethod
    def _require_active_session(operation_name: str) -> None:
        """Require an active SparkSession for the operation.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        from sparkless.session import SparkSession

        if not SparkSession._has_active_session():
            raise RuntimeError(
                f"Cannot perform {operation_name}: "
                "No active SparkSession found. "
                "This operation requires an active SparkSession, similar to PySpark. "
                "Create a SparkSession first: spark = SparkSession('app_name')"
            )

    @staticmethod
    def current_timestamp() -> ColumnOperation:
        """Get current timestamp.

        Returns:
            ColumnOperation representing the current_timestamp function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        DateTimeFunctions._require_active_session("current_timestamp function")
        # Create a ColumnOperation without a column (None for functions without input)
        operation = ColumnOperation(
            None, "current_timestamp", name="current_timestamp()"
        )
        return operation

    @staticmethod
    def current_date() -> ColumnOperation:
        """Get current date.

        Returns:
            ColumnOperation representing the current_date function.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        DateTimeFunctions._require_active_session("current_date function")
        # Create a ColumnOperation without a column (None for functions without input)
        operation = ColumnOperation(None, "current_date", name="current_date()")
        return operation

    @staticmethod
    def now() -> ColumnOperation:
        """Alias for current_timestamp - Get current timestamp.

        Returns:
            ColumnOperation representing the now function.
        """
        return DateTimeFunctions.current_timestamp()

    @staticmethod
    def curdate() -> ColumnOperation:
        """Alias for current_date - Get current date.

        Returns:
            ColumnOperation representing the curdate function.
        """
        return DateTimeFunctions.current_date()

    @staticmethod
    def days(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to days interval.

        Args:
            column: The number of days (can be column or literal).

        Returns:
            ColumnOperation representing the days function.
        """
        if isinstance(column, (str, int)):
            from sparkless.functions.base import Column

            column = Column(str(column)) if isinstance(column, int) else Column(column)

        operation = ColumnOperation(column, "days", name=f"days({column.name})")
        return operation

    @staticmethod
    def hours(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to hours interval.

        Args:
            column: The number of hours (can be column or literal).

        Returns:
            ColumnOperation representing the hours function.
        """
        if isinstance(column, (str, int)):
            from sparkless.functions.base import Column

            column = Column(str(column)) if isinstance(column, int) else Column(column)

        operation = ColumnOperation(column, "hours", name=f"hours({column.name})")
        return operation

    @staticmethod
    def months(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to months interval.

        Args:
            column: The number of months (can be column or literal).

        Returns:
            ColumnOperation representing the months function.
        """
        if isinstance(column, (str, int)):
            from sparkless.functions.base import Column

            column = Column(str(column)) if isinstance(column, int) else Column(column)

        operation = ColumnOperation(column, "months", name=f"months({column.name})")
        return operation

    @staticmethod
    def years(column: Union[Column, str, int]) -> ColumnOperation:
        """Convert number to years interval.

        Args:
            column: The number of years (can be column or literal).

        Returns:
            ColumnOperation representing the years function.
        """
        if isinstance(column, (str, int)):
            from sparkless.functions.base import Column

            column = Column(str(column)) if isinstance(column, int) else Column(column)

        operation = ColumnOperation(column, "years", name=f"years({column.name})")
        return operation

    @staticmethod
    def localtimestamp() -> ColumnOperation:
        """Get local timestamp (without timezone).

        Returns:
            ColumnOperation representing the localtimestamp function.
        """
        operation = ColumnOperation(None, "localtimestamp", name="localtimestamp()")
        return operation

    @staticmethod
    def dateadd(
        date_part: str, value: Union[Column, str, int], date: Union[Column, str]
    ) -> ColumnOperation:
        """SQL Server style date addition.

        Args:
            date_part: The date part to add (year, month, day, etc.).
            value: The value to add.
            date: The date column.

        Returns:
            ColumnOperation representing the dateadd function.
        """
        from sparkless.functions.base import Column

        if isinstance(date, str):
            date = Column(date)
        if isinstance(value, (str, int)):
            value = Column(str(value)) if isinstance(value, int) else Column(value)

        operation = ColumnOperation(
            date,
            "dateadd",
            (date_part, value),
            name=f"dateadd({date_part}, {value.name if hasattr(value, 'name') else value}, {date.name})",
        )
        return operation

    @staticmethod
    def datepart(date_part: str, date: Union[Column, str]) -> ColumnOperation:
        """SQL Server style date part extraction.

        Args:
            date_part: The date part to extract (year, month, day, etc.).
            date: The date column.

        Returns:
            ColumnOperation representing the datepart function.
        """
        if isinstance(date, str):
            date = Column(date)

        operation = ColumnOperation(
            date, "datepart", date_part, name=f"datepart({date_part}, {date.name})"
        )
        return operation

    @staticmethod
    def make_timestamp(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create timestamp from components.

        Args:
            year: Year component.
            month: Month component.
            day: Day component.
            hour: Hour component (default 0).
            minute: Minute component (default 0).
            second: Second component (default 0).

        Returns:
            ColumnOperation representing the make_timestamp function.
        """
        # Convert all to Column if needed
        from sparkless.functions.base import Column

        year_col = (
            Column(str(year))
            if isinstance(year, int)
            else (Column(year) if isinstance(year, str) else year)
        )
        month_col = (
            Column(str(month))
            if isinstance(month, int)
            else (Column(month) if isinstance(month, str) else month)
        )
        day_col = (
            Column(str(day))
            if isinstance(day, int)
            else (Column(day) if isinstance(day, str) else day)
        )
        hour_col = (
            Column(str(hour))
            if isinstance(hour, int)
            else (Column(hour) if isinstance(hour, str) else hour)
        )
        minute_col = (
            Column(str(minute))
            if isinstance(minute, int)
            else (Column(minute) if isinstance(minute, str) else minute)
        )
        second_col = (
            Column(str(second))
            if isinstance(second, int)
            else (Column(second) if isinstance(second, str) else second)
        )

        operation = ColumnOperation(
            year_col,
            "make_timestamp",
            (month_col, day_col, hour_col, minute_col, second_col),
            name="make_timestamp(...)",
        )
        return operation

    @staticmethod
    def make_timestamp_ltz(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
        timezone: Optional[str] = None,
    ) -> ColumnOperation:
        """Create timestamp with local timezone.

        Args:
            year: Year component.
            month: Month component.
            day: Day component.
            hour: Hour component (default 0).
            minute: Minute component (default 0).
            second: Second component (default 0).
            timezone: Optional timezone string.

        Returns:
            ColumnOperation representing the make_timestamp_ltz function.
        """
        from sparkless.functions.base import Column

        year_col = (
            Column(str(year))
            if isinstance(year, int)
            else (Column(year) if isinstance(year, str) else year)
        )
        month_col = (
            Column(str(month))
            if isinstance(month, int)
            else (Column(month) if isinstance(month, str) else month)
        )
        day_col = (
            Column(str(day))
            if isinstance(day, int)
            else (Column(day) if isinstance(day, str) else day)
        )
        hour_col = (
            Column(str(hour))
            if isinstance(hour, int)
            else (Column(hour) if isinstance(hour, str) else hour)
        )
        minute_col = (
            Column(str(minute))
            if isinstance(minute, int)
            else (Column(minute) if isinstance(minute, str) else minute)
        )
        second_col = (
            Column(str(second))
            if isinstance(second, int)
            else (Column(second) if isinstance(second, str) else second)
        )

        operation = ColumnOperation(
            year_col,
            "make_timestamp_ltz",
            (month_col, day_col, hour_col, minute_col, second_col, timezone),
            name="make_timestamp_ltz(...)",
        )
        return operation

    @staticmethod
    def make_timestamp_ntz(
        year: Union[Column, str, int],
        month: Union[Column, str, int],
        day: Union[Column, str, int],
        hour: Union[Column, str, int] = 0,
        minute: Union[Column, str, int] = 0,
        second: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create timestamp with no timezone.

        Args:
            year: Year component.
            month: Month component.
            day: Day component.
            hour: Hour component (default 0).
            minute: Minute component (default 0).
            second: Second component (default 0).

        Returns:
            ColumnOperation representing the make_timestamp_ntz function.
        """
        from sparkless.functions.base import Column

        year_col = (
            Column(str(year))
            if isinstance(year, int)
            else (Column(year) if isinstance(year, str) else year)
        )
        month_col = (
            Column(str(month))
            if isinstance(month, int)
            else (Column(month) if isinstance(month, str) else month)
        )
        day_col = (
            Column(str(day))
            if isinstance(day, int)
            else (Column(day) if isinstance(day, str) else day)
        )
        hour_col = (
            Column(str(hour))
            if isinstance(hour, int)
            else (Column(hour) if isinstance(hour, str) else hour)
        )
        minute_col = (
            Column(str(minute))
            if isinstance(minute, int)
            else (Column(minute) if isinstance(minute, str) else minute)
        )
        second_col = (
            Column(str(second))
            if isinstance(second, int)
            else (Column(second) if isinstance(second, str) else second)
        )

        operation = ColumnOperation(
            year_col,
            "make_timestamp_ntz",
            (month_col, day_col, hour_col, minute_col, second_col),
            name="make_timestamp_ntz(...)",
        )
        return operation

    @staticmethod
    def make_interval(
        years: Union[Column, str, int] = 0,
        months: Union[Column, str, int] = 0,
        weeks: Union[Column, str, int] = 0,
        days: Union[Column, str, int] = 0,
        hours: Union[Column, str, int] = 0,
        mins: Union[Column, str, int] = 0,
        secs: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create interval from components.

        Args:
            years: Years component (default 0).
            months: Months component (default 0).
            weeks: Weeks component (default 0).
            days: Days component (default 0).
            hours: Hours component (default 0).
            mins: Minutes component (default 0).
            secs: Seconds component (default 0).

        Returns:
            ColumnOperation representing the make_interval function.
        """
        from sparkless.functions.base import Column

        # Convert all to Column if needed
        years_col = (
            Column(str(years))
            if isinstance(years, int)
            else (Column(years) if isinstance(years, str) else years)
        )
        months_col = (
            Column(str(months))
            if isinstance(months, int)
            else (Column(months) if isinstance(months, str) else months)
        )
        weeks_col = (
            Column(str(weeks))
            if isinstance(weeks, int)
            else (Column(weeks) if isinstance(weeks, str) else weeks)
        )
        days_col = (
            Column(str(days))
            if isinstance(days, int)
            else (Column(days) if isinstance(days, str) else days)
        )
        hours_col = (
            Column(str(hours))
            if isinstance(hours, int)
            else (Column(hours) if isinstance(hours, str) else hours)
        )
        mins_col = (
            Column(str(mins))
            if isinstance(mins, int)
            else (Column(mins) if isinstance(mins, str) else mins)
        )
        secs_col = (
            Column(str(secs))
            if isinstance(secs, int)
            else (Column(secs) if isinstance(secs, str) else secs)
        )

        operation = ColumnOperation(
            years_col,
            "make_interval",
            (months_col, weeks_col, days_col, hours_col, mins_col, secs_col),
            name="make_interval(...)",
        )
        return operation

    @staticmethod
    def make_dt_interval(
        days: Union[Column, str, int] = 0,
        hours: Union[Column, str, int] = 0,
        mins: Union[Column, str, int] = 0,
        secs: Union[Column, str, int] = 0,
    ) -> ColumnOperation:
        """Create day-time interval.

        Args:
            days: Days component (default 0).
            hours: Hours component (default 0).
            mins: Minutes component (default 0).
            secs: Seconds component (default 0).

        Returns:
            ColumnOperation representing the make_dt_interval function.
        """
        from sparkless.functions.base import Column

        days_col = (
            Column(str(days))
            if isinstance(days, int)
            else (Column(days) if isinstance(days, str) else days)
        )
        hours_col = (
            Column(str(hours))
            if isinstance(hours, int)
            else (Column(hours) if isinstance(hours, str) else hours)
        )
        mins_col = (
            Column(str(mins))
            if isinstance(mins, int)
            else (Column(mins) if isinstance(mins, str) else mins)
        )
        secs_col = (
            Column(str(secs))
            if isinstance(secs, int)
            else (Column(secs) if isinstance(secs, str) else secs)
        )

        operation = ColumnOperation(
            days_col,
            "make_dt_interval",
            (hours_col, mins_col, secs_col),
            name="make_dt_interval(...)",
        )
        return operation

    @staticmethod
    def make_ym_interval(
        years: Union[Column, str, int] = 0, months: Union[Column, str, int] = 0
    ) -> ColumnOperation:
        """Create year-month interval.

        Args:
            years: Years component (default 0).
            months: Months component (default 0).

        Returns:
            ColumnOperation representing the make_ym_interval function.
        """
        from sparkless.functions.base import Column

        years_col = (
            Column(str(years))
            if isinstance(years, int)
            else (Column(years) if isinstance(years, str) else years)
        )
        months_col = (
            Column(str(months))
            if isinstance(months, int)
            else (Column(months) if isinstance(months, str) else months)
        )

        operation = ColumnOperation(
            years_col, "make_ym_interval", months_col, name="make_ym_interval(...)"
        )
        return operation

    @staticmethod
    def to_number(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to number.

        Args:
            column: The column to convert.
            format: Optional format string.

        Returns:
            ColumnOperation representing the to_number function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "to_number", format, name=f"to_number({column.name})"
        )
        return operation

    @staticmethod
    def to_binary(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to binary format.

        Args:
            column: The column to convert.
            format: Optional format string.

        Returns:
            ColumnOperation representing the to_binary function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "to_binary", format, name=f"to_binary({column.name})"
        )
        return operation

    @staticmethod
    def to_unix_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to unix timestamp.

        Args:
            column: The column to convert.
            format: Optional format string.

        Returns:
            ColumnOperation representing the to_unix_timestamp function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "to_unix_timestamp",
            format,
            name=f"to_unix_timestamp({column.name})",
        )
        return operation

    @staticmethod
    def unix_date(column: Union[Column, str]) -> ColumnOperation:
        """Convert unix timestamp to date.

        Args:
            column: The unix timestamp column.

        Returns:
            ColumnOperation representing the unix_date function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "unix_date", name=f"unix_date({column.name})"
        )
        return operation

    @staticmethod
    def unix_seconds(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix seconds.

        Args:
            column: The timestamp column.

        Returns:
            ColumnOperation representing the unix_seconds function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "unix_seconds", name=f"unix_seconds({column.name})"
        )
        return operation

    @staticmethod
    def unix_millis(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix milliseconds.

        Args:
            column: The timestamp column.

        Returns:
            ColumnOperation representing the unix_millis function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "unix_millis", name=f"unix_millis({column.name})"
        )
        return operation

    @staticmethod
    def unix_micros(column: Union[Column, str]) -> ColumnOperation:
        """Convert timestamp to unix microseconds.

        Args:
            column: The timestamp column.

        Returns:
            ColumnOperation representing the unix_micros function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "unix_micros", name=f"unix_micros({column.name})"
        )
        return operation

    @staticmethod
    def timestamp_millis(column: Union[Column, str]) -> ColumnOperation:
        """Create timestamp from unix milliseconds.

        Args:
            column: The unix milliseconds column.

        Returns:
            ColumnOperation representing the timestamp_millis function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "timestamp_millis", name=f"timestamp_millis({column.name})"
        )
        return operation

    @staticmethod
    def timestamp_micros(column: Union[Column, str]) -> ColumnOperation:
        """Create timestamp from unix microseconds.

        Args:
            column: The unix microseconds column.

        Returns:
            ColumnOperation representing the timestamp_micros function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "timestamp_micros", name=f"timestamp_micros({column.name})"
        )
        return operation

    @staticmethod
    def to_date(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string, timestamp, or date to date.

        Args:
            column: The column to convert (StringType, TimestampType, or DateType).
            format: Optional date format string (only used for StringType input).

        Returns:
            ColumnOperation representing the to_date function.

        Raises:
            TypeError: If input column type is not StringType, TimestampType, or DateType
        """
        from sparkless.spark_types import StringType, TimestampType, DateType

        if isinstance(column, str):
            column = Column(column)

        # PySpark accepts StringType, TimestampType, or DateType for to_date
        # Check if we can determine the column type
        input_type = getattr(column, "column_type", None)
        if input_type is not None and not isinstance(
            input_type, (StringType, TimestampType, DateType)
        ):
            raise TypeError(
                f"to_date() requires StringType, TimestampType, or DateType input, got {input_type}. "
                f"Cast the column to string first: F.col('{column.name}').cast('string')"
            )

        name = (
            f"to_date({column.name}, '{format}')"
            if format is not None
            else f"to_date({column.name})"
        )
        operation = ColumnOperation(column, "to_date", format, name=name)
        return operation

    @staticmethod
    def to_timestamp(
        column: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert to timestamp.

        Args:
            column: The column to convert. Accepts StringType, TimestampType,
                IntegerType, LongType, DateType, or DoubleType (matching PySpark behavior).
            format: Optional timestamp format string (used for StringType input).

        Returns:
            ColumnOperation representing the to_timestamp function.

        Raises:
            TypeError: If input column type is not one of the supported types.
        """
        from sparkless.spark_types import (
            StringType,
            TimestampType,
            IntegerType,
            LongType,
            DateType,
            DoubleType,
        )

        if isinstance(column, str):
            column = Column(column)

        # PySpark accepts multiple input types for to_timestamp:
        # - StringType (with format parameter)
        # - TimestampType (pass-through)
        # - IntegerType/LongType (Unix timestamp in seconds)
        # - DateType (convert Date to Timestamp)
        # - DoubleType (Unix timestamp with decimal seconds)
        input_type = getattr(column, "column_type", None)
        if input_type is not None and not isinstance(
            input_type,
            (StringType, TimestampType, IntegerType, LongType, DateType, DoubleType),
        ):
            raise TypeError(
                f"to_timestamp() requires StringType, TimestampType, IntegerType, "
                f"LongType, DateType, or DoubleType input, got {input_type}."
            )

        # Generate a simple name for the operation
        name = f"to_timestamp_{column.name}"
        operation = ColumnOperation(column, "to_timestamp", format, name=name)
        return operation

    @staticmethod
    def hour(column: Union[Column, str]) -> ColumnOperation:
        """Extract hour from timestamp.

        Args:
            column: The column to extract hour from.

        Returns:
            ColumnOperation representing the hour function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "hour", name=f"hour({column.name})")
        return operation

    @staticmethod
    def day(column: Union[Column, str]) -> ColumnOperation:
        """Extract day from date/timestamp.

        Args:
            column: The column to extract day from.

        Returns:
            ColumnOperation representing the day function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "day", name=f"day({column.name})")
        return operation

    @staticmethod
    def dayofmonth(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of month from date/timestamp (alias for day).

        Args:
            column: The column to extract day from.

        Returns:
            ColumnOperation representing the dayofmonth function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "day", name=f"dayofmonth({column.name})")
        return operation

    @staticmethod
    def month(column: Union[Column, str]) -> ColumnOperation:
        """Extract month from date/timestamp.

        Args:
            column: The column to extract month from.

        Returns:
            ColumnOperation representing the month function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "month", name=f"month({column.name})")
        return operation

    @staticmethod
    def year(column: Union[Column, str]) -> ColumnOperation:
        """Extract year from date/timestamp.

        Args:
            column: The column to extract year from.

        Returns:
            ColumnOperation representing the year function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "year", name=f"year({column.name})")
        return operation

    @staticmethod
    def dayofweek(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of week from date/timestamp.

        Args:
            column: The column to extract day of week from.

        Returns:
            ColumnOperation representing the dayofweek function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "dayofweek", name=f"dayofweek({column.name})"
        )
        return operation

    @staticmethod
    def dayofyear(column: Union[Column, str]) -> ColumnOperation:
        """Extract day of year from date/timestamp.

        Args:
            column: The column to extract day of year from.

        Returns:
            ColumnOperation representing the dayofyear function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "dayofyear", name=f"dayofyear({column.name})"
        )
        return operation

    @staticmethod
    def weekofyear(column: Union[Column, str]) -> ColumnOperation:
        """Extract week of year from date/timestamp.

        Args:
            column: The column to extract week of year from.

        Returns:
            ColumnOperation representing the weekofyear function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "weekofyear", name=f"weekofyear({column.name})"
        )
        return operation

    @staticmethod
    def quarter(column: Union[Column, str]) -> ColumnOperation:
        """Extract quarter from date/timestamp.

        Args:
            column: The column to extract quarter from.

        Returns:
            ColumnOperation representing the quarter function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "quarter", name=f"quarter({column.name})")
        return operation

    @staticmethod
    def minute(column: Union[Column, str]) -> ColumnOperation:
        """Extract minute from timestamp.

        Args:
            column: The column to extract minute from.

        Returns:
            ColumnOperation representing the minute function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "minute", name=f"minute({column.name})")
        return operation

    @staticmethod
    def second(column: Union[Column, str]) -> ColumnOperation:
        """Extract second from timestamp.

        Args:
            column: The column to extract second from.

        Returns:
            ColumnOperation representing the second function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(column, "second", name=f"second({column.name})")
        return operation

    @staticmethod
    def add_months(column: Union[Column, str], num_months: int) -> ColumnOperation:
        """Add months to date/timestamp.

        Args:
            column: The column to add months to.
            num_months: Number of months to add.

        Returns:
            ColumnOperation representing the add_months function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "add_months",
            num_months,
            name=f"add_months({column.name}, {num_months})",
        )
        return operation

    @staticmethod
    def months_between(
        column1: Union[Column, str], column2: Union[Column, str]
    ) -> ColumnOperation:
        """Calculate months between two dates.

        Args:
            column1: The first date column.
            column2: The second date column.

        Returns:
            ColumnOperation representing the months_between function.
        """
        if isinstance(column1, str):
            column1 = Column(column1)
        if isinstance(column2, str):
            column2 = Column(column2)

        operation = ColumnOperation(
            column1,
            "months_between",
            column2,
            name=f"months_between({column1.name}, {column2.name}, true)",
        )
        return operation

    @staticmethod
    def date_add(column: Union[Column, str], days: int) -> ColumnOperation:
        """Add days to date.

        Args:
            column: The column to add days to.
            days: Number of days to add.

        Returns:
            ColumnOperation representing the date_add function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "date_add", days, name=f"date_add({column.name}, {days})"
        )
        return operation

    @staticmethod
    def date_sub(column: Union[Column, str], days: int) -> ColumnOperation:
        """Subtract days from date.

        Args:
            column: The column to subtract days from.
            days: Number of days to subtract.

        Returns:
            ColumnOperation representing the date_sub function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column, "date_sub", days, name=f"date_sub({column.name}, {days})"
        )
        return operation

    @staticmethod
    def date_format(column: Union[Column, str], format: str) -> ColumnOperation:
        """Format date/timestamp as string.

        Args:
            column: The column to format.
            format: Date format string (e.g., 'yyyy-MM-dd').

        Returns:
            ColumnOperation representing the date_format function.
        """
        if isinstance(column, str):
            column = Column(column)

        operation = ColumnOperation(
            column,
            "date_format",
            format,
            name=f"date_format({column.name}, {format})",
        )
        return operation

    @staticmethod
    def from_unixtime(
        column: Union[Column, str], format: str = "yyyy-MM-dd HH:mm:ss"
    ) -> ColumnOperation:
        """Convert unix timestamp to string.

        Args:
            column: The column with unix timestamp.
            format: Date format string (default: 'yyyy-MM-dd HH:mm:ss').

        Returns:
            ColumnOperation representing the from_unixtime function.
        """
        if isinstance(column, str):
            column = Column(column)

        # PySpark doesn't quote the format string in the column name
        operation = ColumnOperation(
            column,
            "from_unixtime",
            format,
            name=f"from_unixtime({column.name}, {format})",
        )
        return operation

    @staticmethod
    def timestampadd(
        unit: str, quantity: Union[int, Column], timestamp: Union[str, Column]
    ) -> ColumnOperation:
        """Add time units to a timestamp.

        Args:
            unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
            quantity: Number of units to add (can be column or integer).
            timestamp: Timestamp column or literal.

        Returns:
            ColumnOperation representing the timestampadd function.

        Example:
            >>> df.select(F.timestampadd("DAY", 7, F.col("created_at")))
            >>> df.select(F.timestampadd("HOUR", F.col("offset"), "2024-01-01"))
        """
        if isinstance(timestamp, str):
            timestamp = Column(timestamp)

        # Handle quantity as column or literal
        quantity_str = quantity.name if isinstance(quantity, Column) else str(quantity)

        operation = ColumnOperation(
            timestamp,
            "timestampadd",
            (unit, quantity),
            name=f"timestampadd('{unit}', {quantity_str}, {timestamp.name})",
        )
        return operation

    @staticmethod
    def timestampdiff(
        unit: str, start: Union[str, Column], end: Union[str, Column]
    ) -> ColumnOperation:
        """Calculate difference between two timestamps.

        Args:
            unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND).
            start: Start timestamp column or literal.
            end: End timestamp column or literal.

        Returns:
            ColumnOperation representing the timestampdiff function.

        Example:
            >>> df.select(F.timestampdiff("DAY", F.col("start_date"), F.col("end_date")))
            >>> df.select(F.timestampdiff("HOUR", "2024-01-01", F.col("end_time")))
        """
        if isinstance(start, str):
            start = Column(start)
        if isinstance(end, str):
            end = Column(end)

        operation = ColumnOperation(
            start,
            "timestampdiff",
            (unit, end),
            name=f"timestampdiff('{unit}', {start.name}, {end.name})",
        )
        return operation

    # Timezone Functions (PySpark 3.2+)

    @staticmethod
    def convert_timezone(
        sourceTz: str, targetTz: str, sourceTs: Union[Column, str]
    ) -> ColumnOperation:
        """Convert timestamp from source to target timezone."""
        if isinstance(sourceTs, str):
            sourceTs = Column(sourceTs)

        return ColumnOperation(
            sourceTs,
            "convert_timezone",
            (sourceTz, targetTz, sourceTs),
            name=f"convert_timezone('{sourceTz}', '{targetTz}', {sourceTs.name})",
        )

    @staticmethod
    def current_timezone() -> ColumnOperation:
        """Get current timezone.

        Raises:
            RuntimeError: If no active SparkSession is available
        """
        DateTimeFunctions._require_active_session("current_timezone function")
        # Create a literal for functions without column input
        from sparkless.functions.core.literals import Literal

        dummy = Literal(1)  # Use literal 1 as dummy input
        return ColumnOperation(
            dummy,
            "current_timezone",
            name="current_timezone()",
        )

    @staticmethod
    def from_utc_timestamp(ts: Union[Column, str], tz: str) -> ColumnOperation:
        """Convert UTC timestamp to given timezone."""
        if isinstance(ts, str):
            ts = Column(ts)

        # PySpark doesn't quote the timezone string in the column name
        # Extract timezone string if it's a Literal
        tz_str = tz
        if hasattr(tz, "value") and hasattr(tz, "data_type"):
            tz_str = str(tz.value)
        elif isinstance(tz, str):
            tz_str = tz

        return ColumnOperation(
            ts,
            "from_utc_timestamp",
            tz,
            name=f"from_utc_timestamp({ts.name}, {tz_str})",
        )

    @staticmethod
    def to_utc_timestamp(ts: Union[Column, str], tz: str) -> ColumnOperation:
        """Convert timestamp from given timezone to UTC."""
        if isinstance(ts, str):
            ts = Column(ts)

        # PySpark doesn't quote the timezone string in the column name
        # Extract timezone string if it's a Literal
        tz_str = tz
        if hasattr(tz, "value") and hasattr(tz, "data_type"):
            tz_str = str(tz.value)
        elif isinstance(tz, str):
            tz_str = tz

        return ColumnOperation(
            ts,
            "to_utc_timestamp",
            tz,
            name=f"to_utc_timestamp({ts.name}, {tz_str})",
        )

    # Date/Time Part Functions (PySpark 3.2+)

    @staticmethod
    def date_part(field: str, source: Union[Column, str]) -> ColumnOperation:
        """Extract a field from a date/timestamp.

        Args:
            field: Field to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.).
            source: Date/timestamp column.

        Returns:
            ColumnOperation representing the date_part function.

        Example:
            >>> df.select(F.date_part("YEAR", F.col("date")))
        """
        if isinstance(source, str):
            source = Column(source)

        return ColumnOperation(
            source,
            "date_part",
            field,
            name=f"date_part('{field}', {source.name})",
        )

    @staticmethod
    def dayname(date: Union[Column, str]) -> ColumnOperation:
        """Get the name of the day of the week.

        Args:
            date: Date column.

        Returns:
            ColumnOperation representing the dayname function.

        Example:
            >>> df.select(F.dayname(F.col("date")))
        """
        if isinstance(date, str):
            date = Column(date)

        return ColumnOperation(date, "dayname", name=f"dayname({date.name})")

    @staticmethod
    def make_date(
        year: Union[
            Column, int, str, Literal
        ],  # str and Literal may be passed at runtime
        month: Union[Column, int, str, Literal],
        day: Union[Column, int, str, Literal],
    ) -> ColumnOperation:
        """Construct a date from year, month, day integers (PySpark 3.0+).

        Args:
            year: Year column or integer
            month: Month column or integer (1-12)
            day: Day column or integer (1-31)

        Returns:
            ColumnOperation representing the make_date function

        Example:
            >>> df.select(F.make_date(F.lit(2024), F.lit(3), F.lit(15)))
        """
        # normalize_date_input and get_expression_name are imported at module level

        year_col = normalize_date_input(year)
        month_col = normalize_date_input(month)
        day_col = normalize_date_input(day)

        return ColumnOperation(
            year_col,
            "make_date",
            value=(month_col, day_col),
            name=f"make_date({get_expression_name(year_col)}, {get_expression_name(month_col)}, {get_expression_name(day_col)})",
        )

    @staticmethod
    def date_trunc(format: str, timestamp: Union[Column, str]) -> ColumnOperation:
        """Truncate timestamp to specified unit (year, month, day, hour, etc.).

        Args:
            format: Truncation unit ('year', 'month', 'day', 'hour', 'minute', 'second')
            timestamp: Timestamp column to truncate

        Returns:
            ColumnOperation representing the date_trunc function

        Example:
            >>> df.select(F.date_trunc('month', F.col('timestamp')))
        """
        if isinstance(timestamp, str):
            timestamp = Column(timestamp)

        return ColumnOperation(
            timestamp,
            "date_trunc",
            value=format,
            name=f"date_trunc({format}, {timestamp.name})",
        )

    @staticmethod
    def datediff(
        end: Union[Column, str, Literal], start: Union[Column, str, Literal]
    ) -> ColumnOperation:
        """Returns number of days between two dates.

        Args:
            end: End date column or literal
            start: Start date column or literal

        Returns:
            ColumnOperation representing the datediff function

        Example:
            >>> df.select(F.datediff(F.col('end_date'), F.lit('2024-01-01')))
        """
        # Handle Literal objects

        end_col: Union[Literal, Column]
        if isinstance(end, Literal):
            # For literals, use the literal as-is
            end_col = end
        elif isinstance(end, str):
            # Check if it looks like a date string (not a column name)
            # If it contains dashes and looks like a date, treat as literal
            # Simple heuristic: "YYYY-MM-DD"
            end_col = Literal(end) if "-" in end and len(end) == 10 else Column(end)
        elif isinstance(end, Column):
            # Already a Column, use as-is
            end_col = end
        else:
            # Fallback: for ColumnOperation or other types, create Column from name
            end_col = Column(str(end))  # type: ignore[unreachable]

        start_col: Union[Literal, Column]
        if isinstance(start, Literal):
            start_col = start
        elif isinstance(start, str):
            # Check if it looks like a date string (not a column name)
            if "-" in start and len(start) == 10:  # Simple heuristic: "YYYY-MM-DD"
                start_col = Literal(start)
            else:
                start_col = Column(start)
        elif isinstance(start, Column):
            # Already a Column, use as-is
            start_col = start
        else:
            # Fallback: for ColumnOperation or other types, create Column from name
            start_col = Column(str(start))  # type: ignore[unreachable]

        end_name = (
            end_col.name
            if hasattr(end_col, "name")
            else str(end_col.value)
            if isinstance(end_col, Literal)
            else str(end_col)
        )
        start_name = (
            start_col.name
            if hasattr(start_col, "name")
            else str(start_col.value)
            if isinstance(start_col, Literal)
            else str(start_col)
        )

        return ColumnOperation(
            end_col,
            "datediff",
            value=start_col,
            name=f"datediff({end_name}, {start_name})",
        )

    @staticmethod
    def date_diff(
        end: Union[Column, str], start: Union[Column, str]
    ) -> ColumnOperation:
        """Alias for datediff - Returns number of days between two dates.

        Args:
            end: End date column
            start: Start date column

        Returns:
            ColumnOperation representing the date_diff function

        Example:
            >>> df.select(F.date_diff(F.col('end_date'), F.col('start_date')))
        """
        # Call datediff directly (same implementation)
        if isinstance(end, str):
            end = Column(end)
        if isinstance(start, str):
            start = Column(start)

        return ColumnOperation(
            end, "datediff", value=start, name=f"date_diff({end.name}, {start.name})"
        )

    @staticmethod
    def unix_timestamp(
        timestamp: Optional[Union[Column, str]] = None,
        format: str = "yyyy-MM-dd HH:mm:ss",
    ) -> ColumnOperation:
        """Convert timestamp string to Unix timestamp (seconds since epoch).

        Args:
            timestamp: Timestamp column (optional, defaults to current timestamp)
            format: Date/time format string

        Returns:
            ColumnOperation representing the unix_timestamp function

        Example:
            >>> df.select(F.unix_timestamp(F.col('timestamp'), 'yyyy-MM-dd'))
        """
        from sparkless.functions.core.literals import Literal

        timestamp_col: Union[Literal, Column]
        if timestamp is None:
            timestamp_col = Literal("current_timestamp")
        elif isinstance(timestamp, str):
            timestamp_col = Column(timestamp)
        elif isinstance(timestamp, (Column, Literal)):
            timestamp_col = timestamp
        else:
            # For ColumnOperation or other types, create Column from name
            timestamp_col = Column(str(timestamp))  # type: ignore[unreachable]

        # Get name safely
        if hasattr(timestamp_col, "name"):
            timestamp_name = timestamp_col.name
        else:
            timestamp_name = "current_timestamp"

        return ColumnOperation(
            timestamp_col,
            "unix_timestamp",
            value=format,
            name=f"unix_timestamp({timestamp_name}, {format})",
        )

    @staticmethod
    def last_day(date: Union[Column, str]) -> ColumnOperation:
        """Returns the last day of the month for a given date.

        Args:
            date: Date column

        Returns:
            ColumnOperation representing the last_day function

        Example:
            >>> df.select(F.last_day(F.col('date')))
        """
        if isinstance(date, str):
            date = Column(date)

        return ColumnOperation(date, "last_day", name=f"last_day({date.name})")

    @staticmethod
    def next_day(date: Union[Column, str], dayOfWeek: str) -> ColumnOperation:
        """Returns the first date which is later than the value of the date column that is on the specified day of the week.

        Args:
            date: Date column
            dayOfWeek: Day of week string (e.g., 'Mon', 'Monday')

        Returns:
            ColumnOperation representing the next_day function

        Example:
            >>> df.select(F.next_day(F.col('date'), 'Monday'))
        """
        if isinstance(date, str):
            date = Column(date)

        return ColumnOperation(
            date,
            "next_day",
            value=dayOfWeek,
            name=f"next_day({date.name}, {dayOfWeek})",
        )

    @staticmethod
    def trunc(date: Union[Column, str], format: str) -> ColumnOperation:
        """Truncate date to specified unit (year, month, etc.).

        Args:
            date: Date column
            format: Truncation format ('year', 'yyyy', 'yy', 'month', 'mon', 'mm')

        Returns:
            ColumnOperation representing the trunc function

        Example:
            >>> df.select(F.trunc(F.col('date'), 'year'))
        """
        if isinstance(date, str):
            date = Column(date)

        return ColumnOperation(
            date, "trunc", value=format, name=f"trunc({date.name}, {format})"
        )

    @staticmethod
    def timestamp_seconds(col: Union[Column, str, int]) -> ColumnOperation:
        """Convert seconds since epoch to timestamp (PySpark 3.1+).

        Args:
            col: Column or integer representing seconds since epoch

        Returns:
            ColumnOperation representing the timestamp

        Example:
            >>> df.select(F.timestamp_seconds(F.col("seconds")))
        """

        # Normalize input - preserve Literal, convert int/str to Column
        col_obj = normalize_date_input(col)

        # Extract value/name for ColumnOperation name generation
        col_name = get_expression_name(col_obj)

        return ColumnOperation(
            col_obj,
            "timestamp_seconds",
            name=f"timestamp_seconds({col_name})",
        )

    @staticmethod
    def weekday(col: Union[Column, str]) -> ColumnOperation:
        """Get the day of week as an integer (0 = Monday, 6 = Sunday) (PySpark 3.5+).

        Args:
            col: Column or column name containing date/timestamp values.

        Returns:
            ColumnOperation representing the weekday function.

        Note:
            Returns 0 for Monday through 6 for Sunday.
        """
        column = Column(col) if isinstance(col, str) else col
        return ColumnOperation(column, "weekday", name=f"weekday({column.name})")

    @staticmethod
    def extract(field: str, source: Union[Column, str]) -> ColumnOperation:
        """Extract a field from a date/timestamp column (PySpark 3.5+).

        Args:
            field: The field to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.)
            source: Column or column name containing date/timestamp values.

        Returns:
            ColumnOperation representing the extract function.

        Example:
            >>> df.select(F.extract("YEAR", F.col("date")))
            >>> df.select(F.extract("MONTH", F.col("timestamp")))
        """
        column = Column(source) if isinstance(source, str) else source
        return ColumnOperation(
            column,
            "extract",
            value=field.upper(),
            name=f"extract({field}, {column.name})",
        )

    @staticmethod
    def date_from_unix_date(days: Union[Column, str, int]) -> ColumnOperation:
        """Convert unix date (days since epoch) to date (PySpark 3.5+).

        Args:
            days: Column or integer representing days since epoch (1970-01-01).

        Returns:
            ColumnOperation representing the date_from_unix_date function.

        Example:
            >>> df.select(F.date_from_unix_date(F.col("days")))
        """
        if isinstance(days, (str, int)):
            from sparkless.functions.base import Column

            days = Column(str(days)) if isinstance(days, int) else Column(days)

        operation = ColumnOperation(
            days, "date_from_unix_date", name=f"date_from_unix_date({days.name})"
        )
        return operation

    @staticmethod
    def to_timestamp_ltz(
        timestamp_str: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to timestamp with local timezone (PySpark 3.5+).

        Args:
            timestamp_str: Column or string containing timestamp.
            format: Optional format string for parsing.

        Returns:
            ColumnOperation representing the to_timestamp_ltz function.

        Example:
            >>> df.select(F.to_timestamp_ltz(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
        """
        if isinstance(timestamp_str, str):
            timestamp_str = Column(timestamp_str)

        if format is not None:
            operation = ColumnOperation(
                timestamp_str,
                "to_timestamp_ltz",
                value=format,
                name=f"to_timestamp_ltz({timestamp_str.name}, '{format}')",
            )
        else:
            operation = ColumnOperation(
                timestamp_str,
                "to_timestamp_ltz",
                name=f"to_timestamp_ltz({timestamp_str.name})",
            )
        return operation

    @staticmethod
    def to_timestamp_ntz(
        timestamp_str: Union[Column, str], format: Optional[str] = None
    ) -> ColumnOperation:
        """Convert string to timestamp with no timezone (PySpark 3.5+).

        Args:
            timestamp_str: Column or string containing timestamp.
            format: Optional format string for parsing.

        Returns:
            ColumnOperation representing the to_timestamp_ntz function.

        Example:
            >>> df.select(F.to_timestamp_ntz(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
        """
        if isinstance(timestamp_str, str):
            timestamp_str = Column(timestamp_str)

        if format is not None:
            operation = ColumnOperation(
                timestamp_str,
                "to_timestamp_ntz",
                value=format,
                name=f"to_timestamp_ntz({timestamp_str.name}, '{format}')",
            )
        else:
            operation = ColumnOperation(
                timestamp_str,
                "to_timestamp_ntz",
                name=f"to_timestamp_ntz({timestamp_str.name})",
            )
        return operation

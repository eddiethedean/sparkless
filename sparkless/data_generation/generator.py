"""
Core data generator for Sparkless.

This module provides the main MockDataGenerator class for generating
test data based on schemas with support for various data types and patterns.
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List
from ..spark_types import (
    StructType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    DataType,
)


class MockDataGenerator:
    """Data generation utilities for Sparkless.

    Provides comprehensive data generation capabilities including schema-based
    generation, data corruption simulation, and realistic data patterns.

    Example:
        >>> generator = MockDataGenerator()
        >>> data = generator.create_test_data(schema, num_rows=100)
        >>> corrupted_data = generator.create_corrupted_data(schema, corruption_rate=0.1)
    """

    @staticmethod
    def create_test_data(
        schema: StructType, num_rows: int = 100, seed: int = 42
    ) -> List[Dict[str, Any]]:
        """Generate test data based on schema.

        Args:
            schema: StructType defining the data structure.
            num_rows: Number of rows to generate.
            seed: Random seed for reproducible data.

        Returns:
            List of dictionaries representing the generated data.
        """
        random.seed(seed)
        data = []

        for _ in range(num_rows):
            row = {}
            for field in schema.fields:
                row[field.name] = MockDataGenerator._generate_field_value(
                    field.dataType
                )
            data.append(row)

        return data

    @staticmethod
    def create_corrupted_data(
        schema: StructType,
        corruption_rate: float = 0.1,
        num_rows: int = 100,
        seed: int = 42,
    ) -> List[Dict[str, Any]]:
        """Generate data with some corruption for error testing.

        Args:
            schema: StructType defining the data structure.
            corruption_rate: Fraction of data to corrupt (0.0 to 1.0).
            num_rows: Number of rows to generate.
            seed: Random seed for reproducible data.

        Returns:
            List of dictionaries representing the corrupted data.
        """
        random.seed(seed)
        data = []

        for _ in range(num_rows):
            row = {}
            for field in schema.fields:
                if random.random() < corruption_rate:
                    # Generate corrupted value
                    row[field.name] = MockDataGenerator._generate_corrupted_value(
                        field.dataType
                    )
                else:
                    # Generate normal value
                    row[field.name] = MockDataGenerator._generate_field_value(
                        field.dataType
                    )
            data.append(row)

        return data

    @staticmethod
    def create_realistic_data(
        schema: StructType, num_rows: int = 100, seed: int = 42
    ) -> List[Dict[str, Any]]:
        """Generate realistic data with proper distributions and patterns.

        Args:
            schema: StructType defining the data structure.
            num_rows: Number of rows to generate.
            seed: Random seed for reproducible data.

        Returns:
            List of dictionaries representing the realistic data.
        """
        random.seed(seed)
        data = []

        # Pre-generate some realistic patterns
        names = MockDataGenerator._generate_names(num_rows)
        emails = MockDataGenerator._generate_emails(num_rows)
        dates = MockDataGenerator._generate_dates(num_rows)

        for i in range(num_rows):
            row = {}
            for field in schema.fields:
                row[field.name] = MockDataGenerator._generate_realistic_field_value(
                    field.dataType, i, names, emails, dates
                )
            data.append(row)

        return data

    @staticmethod
    def _generate_field_value(data_type: DataType) -> Any:
        """Generate a value for a specific data type."""
        if isinstance(data_type, StringType):
            return MockDataGenerator._generate_string()
        elif isinstance(data_type, IntegerType):
            return random.randint(1, 100)
        elif isinstance(data_type, LongType):
            return random.randint(1, 1000000)
        elif isinstance(data_type, DoubleType):
            return round(random.uniform(0, 100), 2)
        elif isinstance(data_type, BooleanType):
            return random.choice([True, False])
        elif isinstance(data_type, DateType):
            return MockDataGenerator._generate_date()
        elif isinstance(data_type, TimestampType):
            return MockDataGenerator._generate_timestamp()
        elif isinstance(data_type, ArrayType):
            return MockDataGenerator._generate_array(data_type.element_type)
        elif isinstance(data_type, MapType):
            return MockDataGenerator._generate_map(
                data_type.key_type, data_type.value_type
            )
        else:
            return None

    @staticmethod
    def _generate_corrupted_value(data_type: DataType) -> Any:
        """Generate a corrupted value for a specific data type."""
        if isinstance(data_type, StringType):
            return random.randint(1, 100)  # Wrong type
        elif isinstance(data_type, IntegerType):
            return "invalid"  # Wrong type
        elif isinstance(data_type, LongType):
            return None  # Null value
        elif isinstance(data_type, DoubleType):
            return "not_a_number"  # Wrong type
        elif isinstance(data_type, BooleanType):
            return 42  # Wrong type
        elif isinstance(data_type, DateType):
            return "invalid_date"  # Wrong type
        elif isinstance(data_type, TimestampType):
            return None  # Null value
        elif isinstance(data_type, ArrayType):
            return "not_an_array"  # Wrong type
        elif isinstance(data_type, MapType):
            return None  # Null value
        else:
            return "corrupted"

    @staticmethod
    def _generate_realistic_field_value(
        data_type: DataType,
        index: int,
        names: List[str],
        emails: List[str],
        dates: List[datetime],
    ) -> Any:
        """Generate a realistic value for a specific data type."""
        if isinstance(data_type, StringType):
            if index < len(names):
                return names[index]
            else:
                return MockDataGenerator._generate_string()
        elif isinstance(data_type, IntegerType):
            return random.randint(18, 65)  # Age-like
        elif isinstance(data_type, LongType):
            return random.randint(1000, 999999)  # ID-like
        elif isinstance(data_type, DoubleType):
            return round(random.uniform(1000, 100000), 2)  # Salary-like
        elif isinstance(data_type, BooleanType):
            return random.choice([True, False])
        elif isinstance(data_type, DateType):
            if index < len(dates):
                return dates[index].date()
            else:
                return MockDataGenerator._generate_date()
        elif isinstance(data_type, TimestampType):
            if index < len(dates):
                return dates[index]
            else:
                return MockDataGenerator._generate_timestamp()
        elif isinstance(data_type, ArrayType):
            return MockDataGenerator._generate_array(data_type.element_type)
        elif isinstance(data_type, MapType):
            return MockDataGenerator._generate_map(
                data_type.key_type, data_type.value_type
            )
        else:
            return None

    @staticmethod
    def _generate_string() -> str:
        """Generate a random string."""
        length = random.randint(5, 15)
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def _generate_date() -> datetime:
        """Generate a random date."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randrange(days_between)
        return start_date + timedelta(days=random_days)

    @staticmethod
    def _generate_timestamp() -> datetime:
        """Generate a random timestamp."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)
        time_between = end_date - start_date
        seconds_between = time_between.total_seconds()
        random_seconds = random.randrange(int(seconds_between))
        return start_date + timedelta(seconds=random_seconds)

    @staticmethod
    def _generate_array(element_type: DataType) -> List[Any]:
        """Generate a random array."""
        length = random.randint(0, 5)
        return [
            MockDataGenerator._generate_field_value(element_type) for _ in range(length)
        ]

    @staticmethod
    def _generate_map(key_type: DataType, value_type: DataType) -> Dict[str, Any]:
        """Generate a random map."""
        length = random.randint(0, 3)
        result = {}
        for _ in range(length):
            key = MockDataGenerator._generate_field_value(key_type)
            value = MockDataGenerator._generate_field_value(value_type)
            result[str(key)] = value
        return result

    @staticmethod
    def _generate_names(num_rows: int) -> List[str]:
        """Generate realistic names."""
        first_names = [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Henry",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
        ]

        names = []
        for _ in range(num_rows):
            first = random.choice(first_names)
            last = random.choice(last_names)
            names.append(f"{first} {last}")
        return names

    @staticmethod
    def _generate_emails(num_rows: int) -> List[str]:
        """Generate realistic email addresses."""
        domains = ["gmail.com", "yahoo.com", "hotmail.com", "example.com"]
        usernames = [
            "alice",
            "bob",
            "charlie",
            "diana",
            "eve",
            "frank",
            "grace",
            "henry",
        ]

        emails = []
        for _ in range(num_rows):
            username = random.choice(usernames)
            domain = random.choice(domains)
            emails.append(f"{username}@{domain}")
        return emails

    @staticmethod
    def _generate_dates(num_rows: int) -> List[datetime]:
        """Generate realistic dates."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)
        time_between = end_date - start_date
        seconds_between = time_between.total_seconds()

        dates = []
        for _ in range(num_rows):
            random_seconds = random.randrange(int(seconds_between))
            dates.append(start_date + timedelta(seconds=random_seconds))
        return dates

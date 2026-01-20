"""
Unit tests for ColumnResolver.

Tests the centralized column name resolution system that respects
spark.sql.caseSensitive configuration.
"""

import pytest
from sparkless.core.column_resolver import ColumnResolver
from sparkless.core.exceptions.analysis import AnalysisException


class TestColumnResolver:
    """Test ColumnResolver functionality."""

    def test_resolve_column_name_case_insensitive_match(self):
        """Test case-insensitive column resolution."""
        available_columns = ["Name", "Age", "City"]
        result = ColumnResolver.resolve_column_name(
            "name", available_columns, case_sensitive=False
        )
        assert result == "Name"

        result = ColumnResolver.resolve_column_name(
            "NAME", available_columns, case_sensitive=False
        )
        assert result == "Name"

        result = ColumnResolver.resolve_column_name(
            "nAmE", available_columns, case_sensitive=False
        )
        assert result == "Name"

    def test_resolve_column_name_case_sensitive_match(self):
        """Test case-sensitive column resolution."""
        available_columns = ["Name", "Age", "City"]
        result = ColumnResolver.resolve_column_name(
            "Name", available_columns, case_sensitive=True
        )
        assert result == "Name"

        result = ColumnResolver.resolve_column_name(
            "name", available_columns, case_sensitive=True
        )
        assert result is None  # Case-sensitive: "name" != "Name"

    def test_resolve_column_name_not_found(self):
        """Test resolution when column doesn't exist."""
        available_columns = ["Name", "Age", "City"]
        result = ColumnResolver.resolve_column_name(
            "NonExistent", available_columns, case_sensitive=False
        )
        assert result is None

        result = ColumnResolver.resolve_column_name(
            "NonExistent", available_columns, case_sensitive=True
        )
        assert result is None

    def test_resolve_column_name_ambiguity(self):
        """Test ambiguity detection when multiple columns differ only by case."""
        available_columns = ["Name", "name", "Age"]

        # Should raise AnalysisException due to ambiguity
        with pytest.raises(AnalysisException, match="Ambiguous column name"):
            ColumnResolver.resolve_column_name(
                "name", available_columns, case_sensitive=False
            )

    def test_resolve_columns_multiple(self):
        """Test resolving multiple column names."""
        available_columns = ["Name", "Age", "City"]
        result = ColumnResolver.resolve_columns(
            ["name", "age"], available_columns, case_sensitive=False
        )
        assert result == {"name": "Name", "age": "Age"}

    def test_resolve_columns_with_ambiguity(self):
        """Test resolving columns with ambiguity."""
        available_columns = ["Name", "name", "Age"]

        with pytest.raises(AnalysisException, match="Ambiguous column name"):
            ColumnResolver.resolve_columns(
                ["name", "age"], available_columns, case_sensitive=False
            )

    def test_resolve_columns_not_found(self):
        """Test resolving columns when one doesn't exist."""
        available_columns = ["Name", "Age", "City"]

        with pytest.raises(AnalysisException, match="not found"):
            ColumnResolver.resolve_columns(
                ["name", "non_existent"], available_columns, case_sensitive=False
            )

    def test_column_exists_case_insensitive(self):
        """Test column_exists with case-insensitive matching."""
        available_columns = ["Name", "Age", "City"]
        assert ColumnResolver.column_exists(
            "name", available_columns, case_sensitive=False
        )
        assert ColumnResolver.column_exists(
            "NAME", available_columns, case_sensitive=False
        )
        assert not ColumnResolver.column_exists(
            "NonExistent", available_columns, case_sensitive=False
        )

    def test_column_exists_case_sensitive(self):
        """Test column_exists with case-sensitive matching."""
        available_columns = ["Name", "Age", "City"]
        assert ColumnResolver.column_exists(
            "Name", available_columns, case_sensitive=True
        )
        assert not ColumnResolver.column_exists(
            "name", available_columns, case_sensitive=True
        )
        assert not ColumnResolver.column_exists(
            "NonExistent", available_columns, case_sensitive=True
        )

    def test_column_exists_ambiguous(self):
        """Test column_exists doesn't raise on ambiguity (returns True if any match)."""
        available_columns = ["Name", "name", "Age"]
        # column_exists should return True if at least one match exists
        # (it doesn't check for ambiguity, just existence)
        assert ColumnResolver.column_exists(
            "name", available_columns, case_sensitive=False
        )

    def test_resolve_column_name_empty_list(self):
        """Test resolution with empty column list."""
        result = ColumnResolver.resolve_column_name(
            "name", [], case_sensitive=False
        )
        assert result is None

    def test_resolve_column_name_special_characters(self):
        """Test resolution with special characters in column names."""
        available_columns = ["col_name", "col-name", "col.name"]
        result = ColumnResolver.resolve_column_name(
            "col_name", available_columns, case_sensitive=False
        )
        assert result == "col_name"

        result = ColumnResolver.resolve_column_name(
            "COL_NAME", available_columns, case_sensitive=False
        )
        assert result == "col_name"

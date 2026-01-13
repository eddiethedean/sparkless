"""Join operations for DataFrame."""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple
from ...spark_types import StructType, StructField

if TYPE_CHECKING:
    from ...dataframe.dataframe import DataFrame


class JoinOperationsStatic:
    """Static utility methods for join operations (legacy - use JoinOperations mixin instead)."""

    @staticmethod
    def cross_join(
        left_df: "DataFrame", right_df: "DataFrame"
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Perform cross join (Cartesian product) between two DataFrames.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Create new schema combining both DataFrames
        new_fields = []
        field_names = set()

        # Add fields from left DataFrame
        for field in left_df.schema.fields:
            new_fields.append(field)
            field_names.add(field.name)

        # Add fields from right DataFrame, handling name conflicts
        for field in right_df.schema.fields:
            if field.name in field_names:
                # Create a unique name for the conflict
                new_name = f"{field.name}_right"
                counter = 1
                while new_name in field_names:
                    new_name = f"{field.name}_right_{counter}"
                    counter += 1
                new_fields.append(StructField(new_name, field.dataType))
                field_names.add(new_name)
            else:
                new_fields.append(field)
                field_names.add(field.name)

        new_schema = StructType(new_fields)

        # Create Cartesian product
        result_data = []
        for left_row in left_df.data:
            for right_row in right_df.data:
                new_row = {}

                # Add fields from left DataFrame
                for field in left_df.schema.fields:
                    new_row[field.name] = left_row.get(field.name)

                # Add fields from right DataFrame, handling name conflicts
                for field in right_df.schema.fields:
                    if field.name in [f.name for f in left_df.schema.fields]:
                        # Find the renamed field
                        renamed: Optional[str] = None
                        for new_field in new_fields:
                            if new_field.name.endswith(
                                "_right"
                            ) and new_field.name.startswith(field.name):
                                renamed = new_field.name
                                break
                        if renamed is not None:
                            new_row[renamed] = right_row.get(field.name)
                    else:
                        new_row[field.name] = right_row.get(field.name)

                result_data.append(new_row)

        return result_data, new_schema

    @staticmethod
    def inner_join(
        left_df: "DataFrame", right_df: "DataFrame", on_columns: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Perform inner join between two DataFrames.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            on_columns: List of column names to join on

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Create new schema combining both schemas
        new_fields = list(left_df.schema.fields)
        for field in right_df.schema.fields:
            # Avoid duplicate field names
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)
            else:
                # Handle field name conflicts by prefixing
                new_fields.append(StructField(f"right_{field.name}", field.dataType))

        new_schema = StructType(new_fields)

        # Perform the join
        joined_data = []
        for left_row in left_df.data:
            for right_row in right_df.data:
                # Check if join condition is met
                join_match = True
                for col in on_columns:
                    if left_row.get(col) != right_row.get(col):
                        join_match = False
                        break

                if join_match:
                    # Combine rows
                    joined_row = left_row.copy()
                    for key, value in right_row.items():
                        # Avoid duplicate column names
                        if key not in joined_row:
                            joined_row[key] = value
                        else:
                            # Handle column name conflicts by prefixing
                            joined_row[f"right_{key}"] = value
                    joined_data.append(joined_row)

        return joined_data, new_schema

    @staticmethod
    def left_join(
        left_df: "DataFrame", right_df: "DataFrame", on_columns: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Perform left join between two DataFrames.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            on_columns: List of column names to join on

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Create new schema combining both schemas
        new_fields = list(left_df.schema.fields)
        for field in right_df.schema.fields:
            # Avoid duplicate field names
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)
            else:
                # Handle field name conflicts by prefixing
                new_fields.append(StructField(f"right_{field.name}", field.dataType))

        new_schema = StructType(new_fields)

        # Perform the join
        joined_data = []
        for left_row in left_df.data:
            matched = False
            for right_row in right_df.data:
                # Check if join condition is met
                join_match = True
                for col in on_columns:
                    if left_row.get(col) != right_row.get(col):
                        join_match = False
                        break

                if join_match:
                    matched = True
                    # Combine rows
                    joined_row = left_row.copy()
                    for key, value in right_row.items():
                        # Avoid duplicate column names
                        if key not in joined_row:
                            joined_row[key] = value
                        else:
                            # Handle column name conflicts by prefixing
                            joined_row[f"right_{key}"] = value
                    joined_data.append(joined_row)

            # If no match found, add left row with null values for right columns
            if not matched:
                joined_row = left_row.copy()
                for field in right_df.schema.fields:
                    if not any(f.name == field.name for f in left_df.schema.fields):
                        joined_row[field.name] = None
                    else:
                        joined_row[f"right_{field.name}"] = None
                joined_data.append(joined_row)

        return joined_data, new_schema

    @staticmethod
    def right_join(
        left_df: "DataFrame", right_df: "DataFrame", on_columns: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Perform right join between two DataFrames.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            on_columns: List of column names to join on

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Right join is equivalent to left join with DataFrames swapped
        return JoinOperationsStatic.left_join(right_df, left_df, on_columns)

    @staticmethod
    def outer_join(
        left_df: "DataFrame", right_df: "DataFrame", on_columns: List[str]
    ) -> Tuple[List[Dict[str, Any]], StructType]:
        """Perform outer join between two DataFrames.

        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            on_columns: List of column names to join on

        Returns:
            Tuple of (result_data, result_schema)
        """
        # Create new schema combining both schemas
        new_fields = list(left_df.schema.fields)
        for field in right_df.schema.fields:
            # Avoid duplicate field names
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)
            else:
                # Handle field name conflicts by prefixing
                new_fields.append(StructField(f"right_{field.name}", field.dataType))

        new_schema = StructType(new_fields)

        # Perform the join
        joined_data = []
        left_matched = set()
        right_matched = set()

        # Find all matches
        for i, left_row in enumerate(left_df.data):
            for j, right_row in enumerate(right_df.data):
                # Check if join condition is met
                join_match = True
                for col in on_columns:
                    if left_row.get(col) != right_row.get(col):
                        join_match = False
                        break

                if join_match:
                    left_matched.add(i)
                    right_matched.add(j)
                    # Combine rows
                    joined_row = left_row.copy()
                    for key, value in right_row.items():
                        # Avoid duplicate column names
                        if key not in joined_row:
                            joined_row[key] = value
                        else:
                            # Handle column name conflicts by prefixing
                            joined_row[f"right_{key}"] = value
                    joined_data.append(joined_row)

        # Add unmatched left rows
        for i, left_row in enumerate(left_df.data):
            if i not in left_matched:
                joined_row = left_row.copy()
                for field in right_df.schema.fields:
                    if not any(f.name == field.name for f in left_df.schema.fields):
                        joined_row[field.name] = None
                    else:
                        joined_row[f"right_{field.name}"] = None
                joined_data.append(joined_row)

        # Add unmatched right rows
        for j, right_row in enumerate(right_df.data):
            if j not in right_matched:
                joined_row = {}
                # Add null values for left columns first
                for field in left_df.schema.fields:
                    joined_row[field.name] = None
                # Add right row values, handling conflicts
                for key, value in right_row.items():
                    if not any(f.name == key for f in left_df.schema.fields):
                        joined_row[key] = value
                    else:
                        # For join columns, use the right value in the main field
                        if key in on_columns:
                            joined_row[key] = value
                        else:
                            joined_row[f"right_{key}"] = value
                joined_data.append(joined_row)

        return joined_data, new_schema

    @staticmethod
    def infer_join_schema(
        left_df: "DataFrame", join_params: Tuple[Any, Any, str]
    ) -> StructType:
        """Infer schema for join operation.

        Args:
            left_df: Left DataFrame
            join_params: Join parameters (other_df, on, how)

        Returns:
            Inferred schema after join
        """
        other_df, on, how = join_params

        # Start with all fields from left DataFrame
        new_fields = left_df.schema.fields.copy()

        # Add ALL fields from right DataFrame
        # In SQL joins, all columns from both tables are available
        for field in other_df.schema.fields:
            # Check if field already exists (same name and type)
            existing_field = next((f for f in new_fields if f.name == field.name), None)
            if existing_field is None:
                # Field doesn't exist, add it
                new_fields.append(field)
            # If field exists with same name, we keep the left one (SQL standard behavior)
            # No need to add the right field

        return StructType(new_fields)

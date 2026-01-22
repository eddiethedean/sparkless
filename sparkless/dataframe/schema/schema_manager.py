"""Schema management and inference for DataFrame operations."""

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast

if TYPE_CHECKING:
    from ...dataframe import DataFrame

from ...spark_types import (
    StructType,
    StructField,
    DataType,
    BooleanType,
    LongType,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
    ByteType,
    ShortType,
    FloatType,
)
from ...functions import Literal, Column, ColumnOperation
from ...core.ddl_adapter import parse_ddl_schema
from ...core.column_resolver import ColumnResolver


class SchemaManager:
    """Manages schema projection and type inference for DataFrame operations.

    This class handles:
    - Schema projection after queued lazy operations
    - Type inference for select operations
    - Type inference for withColumn operations
    - Type inference for join operations
    - Cast type string parsing
    """

    @staticmethod
    def project_schema_with_operations(
        base_schema: StructType,
        operations_queue: List[Tuple[str, Any]],
        case_sensitive: bool = False,
    ) -> StructType:
        """Compute schema after applying queued lazy operations.

        Iterates through operations queue and projects resulting schema
        without materializing data.

        Preserves base schema fields even when data is empty.
        """
        # Ensure base_schema has fields attribute
        if not hasattr(base_schema, "fields"):
            # Fallback to empty schema if fields attribute missing
            fields_map: Dict[str, StructField] = {}
        else:
            # Preserve base schema fields - this works even for empty DataFrames with schemas
            fields_map = {f.name: f for f in base_schema.fields}

        # Track whether we're using list-based fields (for joins with duplicates) or dict-based
        fields_list: Optional[List[StructField]] = None
        using_list = False

        for op_name, op_val in operations_queue:
            if op_name == "filter":
                # no schema change
                continue
            elif op_name == "select":
                if using_list and fields_list is not None:
                    # Convert list back to dict for select operation
                    fields_map = {f.name: f for f in fields_list}
                    fields_list = None
                    using_list = False
                fields_map = SchemaManager._handle_select_operation(
                    fields_map, op_val, case_sensitive
                )
            elif op_name == "withColumn":
                if using_list and fields_list is not None:
                    # Convert list back to dict for withColumn operation
                    fields_map = {f.name: f for f in fields_list}
                    fields_list = None
                    using_list = False
                col_name, col = op_val
                fields_map = SchemaManager._handle_withcolumn_operation(
                    fields_map, col_name, col, base_schema, case_sensitive
                )
            elif op_name == "drop":
                if using_list and fields_list is not None:
                    # Convert list back to dict for drop operation
                    fields_map = {f.name: f for f in fields_list}
                    fields_list = None
                    using_list = False
                fields_map = SchemaManager._handle_drop_operation(
                    fields_map, op_val, case_sensitive
                )
            elif op_name == "withColumnRenamed":
                # Handle column rename - update field names in schema
                if using_list and fields_list is not None:
                    # Convert list back to dict for rename operation
                    fields_map = {f.name: f for f in fields_list}
                    fields_list = None
                    using_list = False
                old_name, new_name = op_val
                # Find actual column name using ColumnResolver
                available_columns = list(fields_map.keys())
                actual_old_name = ColumnResolver.resolve_column_name(
                    old_name, available_columns, case_sensitive
                )
                if actual_old_name:
                    # Rename the field
                    field = fields_map.pop(actual_old_name)
                    # Create new field with new name but same type and nullable
                    fields_map[new_name] = StructField(
                        new_name, field.dataType, field.nullable
                    )
            elif op_name == "join":
                other_df, on, how = op_val
                # For semi/anti joins, only return left DataFrame columns (don't add right columns)
                if how and how.lower() in ("semi", "anti", "left_semi", "left_anti"):
                    # For semi/anti joins, don't add right-side columns
                    # If using list, keep the list as-is (only left columns)
                    # If using dict, keep the dict as-is (only left columns)
                    continue

                # Convert dict to list if this is the first join
                if not using_list:
                    fields_list = list(fields_map.values())
                    using_list = True

                # Determine if join is on column name(s) (string / list of strings)
                # vs a column expression join.
                #
                # Sparkless cannot represent duplicate column names reliably (many operations
                # assume uniqueness). For joins using column names, we therefore deduplicate
                # *all* right-side columns whose names already exist on the left. This also
                # matches the long-standing behavior relied on by parity tests (e.g. issue #128).
                is_column_name_join = isinstance(on, str) or (
                    isinstance(on, list) and all(isinstance(c, str) for c in on)
                )

                # Add fields from right DataFrame
                if fields_list is not None:
                    existing_field_names = {f.name for f in fields_list}
                    for field in other_df.schema.fields:
                        if is_column_name_join and field.name in existing_field_names:
                            # Deduplicate any right-side column that already exists on the left.
                            continue
                        # For column expression joins or non-duplicate columns, add the field
                        fields_list.append(field)
                        existing_field_names.add(field.name)
            elif op_name == "union":
                # Union operation - handle type coercion
                other_df = op_val
                other_schema = other_df.schema

                # Convert dict to list if needed
                if not using_list:
                    fields_list = list(fields_map.values())
                    using_list = True

                if fields_list is not None:
                    # Determine coerced types for each column
                    # Numeric types for coercion logic
                    numeric_types = (
                        ByteType,
                        ShortType,
                        IntegerType,
                        LongType,
                        FloatType,
                        DoubleType,
                    )

                    # Update each field's type based on coercion rules
                    for i, field in enumerate(fields_list):
                        if i < len(other_schema.fields):
                            other_field = other_schema.fields[i]
                            if field.name == other_field.name:
                                # Determine target type after coercion
                                target_type = field.dataType
                                if field.dataType != other_field.dataType:
                                    # Type coercion needed
                                    is_numeric1 = isinstance(
                                        field.dataType, numeric_types
                                    )
                                    is_numeric2 = isinstance(
                                        other_field.dataType, numeric_types
                                    )
                                    is_string1 = isinstance(field.dataType, StringType)
                                    is_string2 = isinstance(
                                        other_field.dataType, StringType
                                    )

                                    if (is_numeric1 and is_string2) or (
                                        is_string1 and is_numeric2
                                    ):
                                        # Numeric + String -> String (PySpark behavior, issue #242)
                                        target_type = StringType()
                                    elif is_numeric1 and is_numeric2:
                                        # Both numeric - promote to wider type
                                        if isinstance(
                                            field.dataType, (FloatType, DoubleType)
                                        ) or isinstance(
                                            other_field.dataType,
                                            (FloatType, DoubleType),
                                        ):
                                            target_type = DoubleType()
                                        elif isinstance(
                                            field.dataType, LongType
                                        ) or isinstance(other_field.dataType, LongType):
                                            target_type = LongType()
                                        else:
                                            # Keep first type or promote to Integer
                                            target_type = field.dataType

                                # Update field with coerced type
                                fields_list[i] = StructField(
                                    field.name, target_type, field.nullable
                                )

        # Return appropriate format
        if using_list and fields_list is not None:
            return StructType(fields_list)
        else:
            return StructType(list(fields_map.values()))

    @staticmethod
    def _handle_select_operation(
        fields_map: Dict[str, StructField],
        columns: Tuple[Any, ...],
        case_sensitive: bool = False,
    ) -> Dict[str, StructField]:
        """Handle select operation schema changes."""
        new_fields_map = {}

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all existing fields
                    new_fields_map.update(fields_map)
                elif "." in col:
                    # Handle nested struct field access (e.g., "Person.name")
                    # Split into struct column and field name
                    parts = col.split(".", 1)
                    struct_col = parts[0]
                    field_name = parts[1]
                    # Check if struct column exists in fields_map
                    if struct_col in fields_map:
                        struct_field = fields_map[struct_col]
                        struct_type = struct_field.dataType
                        if isinstance(struct_type, StructType):
                            # Find the nested field within the struct
                            nested_field = None
                            for f in struct_type.fields:
                                if f.name == field_name:
                                    nested_field = f
                                    break
                            if nested_field:
                                # Create a new field with the nested field's type
                                new_fields_map[col] = StructField(
                                    col, nested_field.dataType, nested_field.nullable
                                )
                    # If nested field not found, infer type as String (fallback)
                    # This handles cases where the field name doesn't match exactly
                    elif struct_col in fields_map:
                        struct_field = fields_map[struct_col]
                        struct_type = struct_field.dataType
                        if isinstance(struct_type, StructType):
                            # Try case-insensitive match
                            from ...core.column_resolver import ColumnResolver

                            field_names = [f.name for f in struct_type.fields]
                            resolved_field = ColumnResolver.resolve_column_name(
                                field_name, field_names, False
                            )
                            if resolved_field:
                                for f in struct_type.fields:
                                    if f.name == resolved_field:
                                        new_fields_map[col] = StructField(
                                            col, f.dataType, f.nullable
                                        )
                                        break
                    else:
                        # Struct column doesn't exist - infer as String (fallback)
                        new_fields_map[col] = StructField(col, StringType(), True)
                else:
                    # Resolve column name to find actual field
                    # PySpark behavior:
                    # - If there's only one match: use the original column name
                    # - If there are multiple matches (different cases): use the requested column name
                    from ...core.column_resolver import ColumnResolver

                    resolved_col_name = ColumnResolver.resolve_column_name(
                        col, list(fields_map.keys()), case_sensitive
                    )
                    if resolved_col_name:
                        # Check if there are multiple matches (different cases)
                        column_name_lower = col.lower()
                        matches = [
                            c for c in fields_map if c.lower() == column_name_lower
                        ]
                        has_multiple_matches = len(matches) > 1

                        resolved_field = fields_map[resolved_col_name]
                        # Use original column name if single match, requested name if multiple matches
                        output_col_name = (
                            col if has_multiple_matches else resolved_col_name
                        )
                        new_fields_map[output_col_name] = StructField(
                            output_col_name,
                            resolved_field.dataType,
                            resolved_field.nullable,
                        )
                    else:
                        # Column not found - infer as String (fallback)
                        new_fields_map[col] = StructField(col, StringType(), True)
            elif hasattr(col, "name"):
                col_name = col.name
                if col_name == "*":
                    # Add all existing fields
                    new_fields_map.update(fields_map)
                elif col_name in fields_map:
                    new_fields_map[col_name] = fields_map[col_name]
                elif isinstance(col, Literal):
                    # For Literal objects - literals are never nullable
                    new_fields_map[col_name] = SchemaManager._create_literal_field(col)
                elif isinstance(col, ColumnOperation) and col.operation == "json_tuple":
                    # json_tuple(col, *fields) expands into multiple string columns (c0, c1, ...)
                    fields = (
                        list(col.value) if isinstance(col.value, (list, tuple)) else []
                    )
                    for i in range(len(fields)):
                        name = f"c{i}"
                        new_fields_map[name] = StructField(name, StringType(), True)
                else:
                    # New column from expression - infer type based on operation
                    new_fields_map[col_name] = SchemaManager._infer_expression_type(col)

        return new_fields_map

    @staticmethod
    def _handle_withcolumn_operation(
        fields_map: Dict[str, StructField],
        col_name: str,
        col: Union[Column, ColumnOperation, Literal, Any],
        base_schema: StructType,
        case_sensitive: bool = False,
    ) -> Dict[str, StructField]:
        """Handle withColumn operation schema changes."""
        col_any = cast("Any", col)
        operation = getattr(col_any, "operation", None)

        if operation is not None and hasattr(col_any, "name"):
            if operation == "cast":
                # Cast operation - use the target data type from col.value
                # This handles F.lit(None).cast(TimestampType()) correctly
                cast_type = getattr(col_any, "value", None)
                if isinstance(cast_type, str):
                    fields_map[col_name] = StructField(
                        col_name,
                        SchemaManager.parse_cast_type_string(cast_type),
                        nullable=True,
                    )
                else:
                    # Already a DataType object
                    if isinstance(cast_type, DataType):
                        # Create a new instance with nullable=True
                        # This is critical for F.lit(None).cast() - the column should be nullable
                        type_class = type(cast_type)
                        if type_class in (
                            StringType,
                            IntegerType,
                            LongType,
                            DoubleType,
                            BooleanType,
                            DateType,
                            TimestampType,
                        ):
                            # Simple types that take nullable parameter
                            new_type = type_class(nullable=True)
                        elif isinstance(cast_type, ArrayType):
                            new_type = ArrayType(cast_type.element_type, nullable=True)
                        elif isinstance(cast_type, MapType):
                            new_type = MapType(
                                cast_type.key_type, cast_type.value_type, nullable=True
                            )
                        else:
                            # For other types, try to preserve nullable=True
                            try:
                                new_type = type_class(nullable=True)
                            except (TypeError, ValueError):
                                # If type doesn't support nullable parameter, use as-is
                                new_type = cast_type
                        fields_map[col_name] = StructField(
                            col_name, new_type, nullable=True
                        )
                    elif cast_type is not None:
                        fields_map[col_name] = StructField(
                            col_name, cast_type, nullable=True
                        )
                    else:
                        fields_map[col_name] = StructField(
                            col_name, StringType(), nullable=True
                        )
            elif operation in ["+", "-", "*", "/", "%"]:
                # Arithmetic operations - infer type from operands
                data_type = SchemaManager._infer_arithmetic_type(col_any, base_schema)
                fields_map[col_name] = StructField(col_name, data_type)
            elif operation in ["abs"]:
                fields_map[col_name] = StructField(col_name, LongType())
            elif operation in ["length"]:
                fields_map[col_name] = StructField(col_name, IntegerType())
            elif operation in ["round"]:
                data_type = SchemaManager._infer_round_type(col_any)
                fields_map[col_name] = StructField(col_name, data_type)
            elif operation in ["upper", "lower"]:
                fields_map[col_name] = StructField(col_name, StringType())
            elif operation == "datediff":
                fields_map[col_name] = StructField(col_name, IntegerType())
            elif operation == "months_between":
                fields_map[col_name] = StructField(col_name, DoubleType())
            elif operation in [
                "hour",
                "minute",
                "second",
                "day",
                "dayofmonth",
                "month",
                "year",
                "quarter",
                "dayofweek",
                "dayofyear",
                "weekofyear",
            ]:
                fields_map[col_name] = StructField(col_name, IntegerType())
            elif operation in ("from_json", "from_csv"):
                struct_type = SchemaManager._resolve_struct_type(col_any)
                fields_map[col_name] = StructField(
                    col_name, struct_type if struct_type is not None else StructType([])
                )
            elif operation in ("to_json", "to_csv"):
                alias = SchemaManager._build_function_alias(
                    operation, col_any.column, col_name
                )
                fields_map[col_name] = StructField(alias, StringType())
            elif operation == "to_date":
                # to_date returns DateType
                fields_map[col_name] = StructField(col_name, DateType())
            elif operation == "to_timestamp":
                # to_timestamp returns TimestampType
                fields_map[col_name] = StructField(col_name, TimestampType())
            elif operation == "withField":
                # withField operation - add or replace field in struct
                # Get the base struct column's schema
                base_col = getattr(col_any, "column", None)
                if base_col is None:
                    # Can't determine base column - default to StringType
                    fields_map[col_name] = StructField(col_name, StringType())
                else:
                    # Get base column name
                    base_col_name = (
                        base_col.name if hasattr(base_col, "name") else str(base_col)
                    )

                    # Find the base struct column in fields_map using ColumnResolver
                    available_columns = list(fields_map.keys())
                    actual_base_col_name = ColumnResolver.resolve_column_name(
                        base_col_name, available_columns, case_sensitive
                    )
                    base_struct_field = (
                        fields_map.get(actual_base_col_name)
                        if actual_base_col_name
                        else None
                    )

                    if base_struct_field is None:
                        # Base column not found - default to StringType
                        fields_map[col_name] = StructField(col_name, StringType())
                    else:
                        # Get the struct type
                        if not isinstance(base_struct_field.dataType, StructType):
                            # Base column is not a struct - default to StringType
                            fields_map[col_name] = StructField(col_name, StringType())
                        else:
                            # Extract field name and column from operation value
                            if (
                                not isinstance(col_any.value, dict)
                                or "fieldName" not in col_any.value
                            ):
                                # Invalid withField operation - use base struct type
                                fields_map[col_name] = StructField(
                                    col_name, base_struct_field.dataType
                                )
                            else:
                                field_name = col_any.value["fieldName"]
                                field_column = col_any.value.get("column")

                                # Infer the new field's data type
                                if field_column is None:
                                    # No field column - default to StringType
                                    field_data_type = StringType()
                                elif isinstance(field_column, Literal):
                                    # Literal - infer type from literal
                                    field_data_type = (
                                        SchemaManager._create_literal_field(
                                            field_column
                                        ).dataType
                                    )
                                elif hasattr(field_column, "operation"):
                                    # ColumnOperation - infer type from operation
                                    field_data_type = (
                                        SchemaManager._infer_expression_type(
                                            field_column
                                        )
                                    )
                                else:
                                    # Simple column reference - get type from schema
                                    field_data_type = StringType()  # Default

                                # Create new struct type with updated/added field
                                new_fields = list(base_struct_field.dataType.fields)

                                # Check if field already exists
                                field_exists = False
                                for i, existing_field in enumerate(new_fields):
                                    if existing_field.name == field_name:
                                        # Replace existing field
                                        new_fields[i] = StructField(
                                            field_name, field_data_type, nullable=True
                                        )
                                        field_exists = True
                                        break

                                if not field_exists:
                                    # Add new field
                                    new_fields.append(
                                        StructField(
                                            field_name, field_data_type, nullable=True
                                        )
                                    )

                                # Create new struct type
                                new_struct_type = StructType(new_fields)
                                fields_map[col_name] = StructField(
                                    col_name,
                                    new_struct_type,
                                    base_struct_field.nullable,
                                )
            else:
                fields_map[col_name] = StructField(col_name, StringType())
        elif isinstance(col, Literal):
            # For Literal objects - check if it's been cast
            # If the literal's value is None, it should be nullable
            if col.value is None:
                # None literals should be nullable, but we need to check if there's a cast
                # Check if this Literal is wrapped in a ColumnOperation with cast
                # This is handled by the operation check above, but if we reach here,
                # it means the Literal itself is being used directly
                # For None literals, default to StringType but make it nullable
                fields_map[col_name] = StructField(
                    col_name, col.data_type, nullable=True
                )
            else:
                # For non-None literals, they are never nullable
                field = SchemaManager._create_literal_field(col)
                fields_map[col_name] = StructField(
                    col_name, field.dataType, field.nullable
                )
        else:
            # fallback literal inference
            data_type = SchemaManager._infer_literal_type(col_any)
            fields_map[col_name] = StructField(col_name, data_type)

        return fields_map

    @staticmethod
    def _handle_drop_operation(
        fields_map: Dict[str, StructField],
        columns_to_drop: Union[str, List[str], Tuple[str, ...]],
        case_sensitive: bool = False,
    ) -> Dict[str, StructField]:
        """Handle drop operation schema changes.

        Args:
            fields_map: Current schema fields map
            columns_to_drop: Column name(s) to drop (string, list, or tuple)

        Returns:
            Updated fields_map with dropped columns removed
        """
        # Handle different formats for columns_to_drop
        if isinstance(columns_to_drop, str):
            # Single column name
            columns_to_drop = [columns_to_drop]
        elif isinstance(columns_to_drop, tuple):
            # Convert tuple to list
            columns_to_drop = list(columns_to_drop)

        # Remove columns from fields_map using ColumnResolver
        available_columns = list(fields_map.keys())
        for col_name in columns_to_drop:
            # Find actual column name using ColumnResolver
            actual_col_name = ColumnResolver.resolve_column_name(
                col_name, available_columns, case_sensitive
            )
            if actual_col_name:
                del fields_map[actual_col_name]

        return fields_map

    @staticmethod
    def _handle_join_operation(
        fields_map: Dict[str, StructField],
        other_df: "DataFrame",
        how: str = "inner",
    ) -> Dict[str, StructField]:
        """Handle join operation schema changes."""
        # For semi/anti joins, only return left DataFrame columns
        if how and how.lower() in ["semi", "anti", "left_semi", "left_anti"]:
            # Don't add right DataFrame fields for semi/anti joins
            return fields_map

        # Add fields from the other DataFrame to the schema
        for field in other_df.schema.fields:
            # Avoid duplicate field names
            if field.name not in fields_map:
                fields_map[field.name] = field
            else:
                # Handle field name conflicts by prefixing
                new_field = StructField(
                    f"right_{field.name}", field.dataType, field.nullable
                )
                fields_map[f"right_{field.name}"] = new_field

        return fields_map

    @staticmethod
    def _create_literal_field(col: Literal) -> StructField:
        """Create a field for a Literal object."""
        col_type = col.column_type
        if isinstance(col_type, BooleanType):
            data_type: DataType = BooleanType(nullable=False)
        elif isinstance(col_type, IntegerType):
            data_type = IntegerType(nullable=False)
        elif isinstance(col_type, LongType):
            data_type = LongType(nullable=False)
        elif isinstance(col_type, DoubleType):
            data_type = DoubleType(nullable=False)
        elif isinstance(col_type, StringType):
            data_type = StringType(nullable=False)
        else:
            # For other types, create a new instance with nullable=False
            data_type = col_type.__class__(nullable=False)

        return StructField(col.name, data_type, nullable=False)

    @staticmethod
    def _infer_expression_type(
        col: Union[Column, ColumnOperation, Literal, Any],
    ) -> StructField:
        """Infer type for an expression column."""
        if hasattr(col, "operation"):
            operation = getattr(col, "operation", None)
            if operation == "datediff":
                return StructField(col.name, IntegerType())
            elif operation == "months_between":
                return StructField(col.name, DoubleType())
            elif operation in [
                "hour",
                "minute",
                "second",
                "day",
                "dayofmonth",
                "month",
                "year",
                "quarter",
                "dayofweek",
                "dayofyear",
                "weekofyear",
            ]:
                return StructField(col.name, IntegerType())
            elif operation in ("from_json", "from_csv"):
                struct_type = SchemaManager._resolve_struct_type(col)
                return StructField(
                    col.name, struct_type if struct_type is not None else StructType([])
                )
            elif operation in ("to_json", "to_csv"):
                alias = SchemaManager._build_function_alias(
                    operation, getattr(col, "column", None), col.name
                )
                return StructField(alias, StringType())
            elif operation == "to_date":
                # to_date returns DateType
                return StructField(col.name, DateType())
            elif operation == "to_timestamp":
                # to_timestamp returns TimestampType
                return StructField(col.name, TimestampType())
            else:
                # Default to StringType for unknown operations
                return StructField(col.name, StringType())
        else:
            # No operation attribute - default to StringType
            return StructField(col.name, StringType())

    @staticmethod
    def _resolve_struct_type(col: Union[ColumnOperation, Any]) -> Optional[StructType]:
        """Extract StructType information from a column operation's schema argument."""
        if not hasattr(col, "value"):
            return None

        value = getattr(col, "value")
        schema_spec: Any = None
        if isinstance(value, tuple) and len(value) >= 1:
            schema_spec = value[0]
        else:
            schema_spec = value

        return SchemaManager._coerce_to_struct_type(schema_spec)

    @staticmethod
    def _coerce_to_struct_type(schema_spec: Any) -> Optional[StructType]:
        """Coerce various schema representations to StructType."""
        if schema_spec is None:
            return None

        if isinstance(schema_spec, StructType):
            return schema_spec

        if isinstance(schema_spec, StructField):
            return StructType([schema_spec])

        if isinstance(schema_spec, Literal):
            return SchemaManager._coerce_to_struct_type(schema_spec.value)

        if hasattr(schema_spec, "value") and not isinstance(
            schema_spec, (dict, list, str)
        ):
            return SchemaManager._coerce_to_struct_type(schema_spec.value)

        if isinstance(schema_spec, str):
            try:
                return parse_ddl_schema(schema_spec)
            except Exception:
                return StructType([])

        if isinstance(schema_spec, dict):
            return StructType([StructField(name, StringType()) for name in schema_spec])

        if isinstance(schema_spec, (list, tuple)):
            collected_fields: List[StructField] = []
            for item in schema_spec:
                if isinstance(item, StructField):
                    collected_fields.append(item)
                elif isinstance(item, str):
                    collected_fields.append(StructField(item, StringType()))
            if collected_fields:
                return StructType(collected_fields)

        return None

    @staticmethod
    def _build_function_alias(operation: str, column_expr: Any, fallback: str) -> str:
        if operation in ("to_json", "to_csv") and column_expr is not None:
            struct_alias = SchemaManager._format_struct_alias(column_expr)
            return f"{operation}({struct_alias})"
        return fallback

    @staticmethod
    def _format_struct_alias(expr: Any) -> str:
        names = SchemaManager._extract_struct_field_names(expr)
        if names:
            return f"struct({', '.join(names)})"
        return "struct(...)"

    @staticmethod
    def _extract_struct_field_names(expr: Any) -> List[str]:
        names: List[str] = []
        if (
            isinstance(expr, ColumnOperation)
            and getattr(expr, "operation", None) == "struct"
        ):
            first = SchemaManager._extract_column_name(expr.column)
            if first:
                names.append(first)
            additional = expr.value
            if isinstance(additional, tuple):
                for item in additional:
                    name = SchemaManager._extract_column_name(item)
                    if name:
                        names.append(name)
        else:
            name = SchemaManager._extract_column_name(expr)
            if name:
                names.append(name)
        return names

    @staticmethod
    def _extract_column_name(expr: Any) -> Optional[str]:
        if isinstance(expr, Column):
            return str(expr.name) if expr.name is not None else None
        if isinstance(expr, ColumnOperation) and hasattr(expr, "name"):
            name = getattr(expr, "name", None)
            return str(name) if name is not None else None
        if isinstance(expr, str):
            return expr
        name = getattr(expr, "name", None)
        return str(name) if name is not None else None

    @staticmethod
    def _infer_arithmetic_type(
        col: Union[Column, ColumnOperation, Any], base_schema: StructType
    ) -> DataType:
        """Infer type for arithmetic operations."""
        left_type = None
        right_type = None

        # Get left operand type (the column itself)
        if hasattr(col, "column"):
            left_operand = getattr(col, "column", None)
            if left_operand is not None and hasattr(left_operand, "name"):
                for field in base_schema.fields:
                    if field.name == left_operand.name:
                        left_type = field.dataType
                        break
        elif hasattr(col, "name"):
            for field in base_schema.fields:
                if field.name == col.name:
                    left_type = field.dataType
                    break

        # Get right operand type
        right_operand = getattr(col, "value", None)
        if right_operand is not None:
            if hasattr(right_operand, "name"):
                # It's a column reference
                for field in base_schema.fields:
                    if field.name == right_operand.name:
                        right_type = field.dataType
                        break
            elif hasattr(right_operand, "value") and hasattr(right_operand, "name"):
                # It's a Literal - infer type from value
                from ...functions.core.literals import Literal

                if isinstance(right_operand, Literal):
                    if isinstance(right_operand.value, float):
                        right_type = DoubleType()
                    elif isinstance(right_operand.value, int):
                        right_type = LongType()
                    else:
                        right_type = StringType()
            elif isinstance(right_operand, (int, float)):
                # It's a numeric literal
                right_type = (
                    DoubleType() if isinstance(right_operand, float) else LongType()
                )

        # PySpark behavior: String columns are automatically cast to Double for arithmetic
        # If either operand is StringType or DoubleType, result is DoubleType
        if (left_type and isinstance(left_type, (StringType, DoubleType))) or (
            right_type and isinstance(right_type, (StringType, DoubleType))
        ):
            return DoubleType()
        else:
            return LongType()

    @staticmethod
    def _infer_round_type(
        col: Union[Column, ColumnOperation, Any],
    ) -> DataType:
        """Infer type for round operation."""
        # round() should return the same type as its input
        col_any = cast("Any", col)
        column_operand = getattr(col_any, "column", None)
        if (
            column_operand is not None
            and hasattr(column_operand, "operation")
            and getattr(column_operand, "operation") == "cast"
        ):
            # If the input is a cast operation, check the target type
            cast_type = getattr(column_operand, "value", "string")
            if isinstance(cast_type, str) and cast_type.lower() in ["int", "integer"]:
                return LongType()
            else:
                return DoubleType()
        else:
            # Default to DoubleType for other cases
            return DoubleType()

    @staticmethod
    def _infer_literal_type(
        col: Union[Literal, int, float, str, bool, Any],
    ) -> DataType:
        """Infer type for literal values."""
        if isinstance(col, (int, float)):
            if isinstance(col, float):
                return DoubleType()
            else:
                return LongType()
        else:
            return StringType()

    @staticmethod
    def parse_cast_type_string(type_str: str) -> DataType:
        """Parse a cast type string to DataType."""
        type_str = type_str.strip().lower()

        # Primitive types
        if type_str in ["int", "integer"]:
            return IntegerType()
        elif type_str in ["long", "bigint"]:
            return LongType()
        elif type_str in ["double", "float"]:
            return DoubleType()
        elif type_str in ["string", "varchar"]:
            return StringType()
        elif type_str in ["boolean", "bool"]:
            return BooleanType()
        elif type_str == "date":
            return DateType()
        elif type_str == "timestamp":
            return TimestampType()
        elif type_str.startswith("decimal"):
            import re

            match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 2)
        elif type_str.startswith("array<"):
            element_type_str = type_str[6:-1]
            return ArrayType(SchemaManager.parse_cast_type_string(element_type_str))
        elif type_str.startswith("map<"):
            types = type_str[4:-1].split(",", 1)
            key_type = SchemaManager.parse_cast_type_string(types[0].strip())
            value_type = SchemaManager.parse_cast_type_string(types[1].strip())
            return MapType(key_type, value_type)
        else:
            return StringType()  # Default fallback

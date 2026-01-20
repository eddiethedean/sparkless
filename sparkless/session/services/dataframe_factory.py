"""
from __future__ import annotations
DataFrame factory service for SparkSession.

This service handles DataFrame creation, schema inference, and validation
following the Single Responsibility Principle.
"""

from typing import Sequence
from typing import Any, Dict, List, Optional, Tuple, Union, cast

try:
    import pandas as pd
except ImportError:
    pd = None
from sparkless.spark_types import (
    StructType,
    StructField,
    StringType,
    DataType,
    Row,
)
from sparkless.dataframe import DataFrame
from sparkless.session.config import SparkConfig
from sparkless.core.exceptions import IllegalArgumentException


class DataFrameFactory:
    """Factory for creating DataFrames with validation and coercion."""

    def create_dataframe(
        self,
        data: Union[List[Dict[str, Any]], List[Any], Any],
        schema: Optional[Union[StructType, List[str], str]],
        engine_config: SparkConfig,
        storage: Any,
    ) -> DataFrame:
        """Create a DataFrame from data.

        Args:
            data: List of dictionaries or tuples representing rows, or a Pandas DataFrame.
                  Pandas DataFrames are detected using duck typing (checks for to_dict
                  method with orient="records" parameter) to work with both real and
                  mock pandas modules.
            schema: Optional schema definition (StructType or list of column names).
                    Required for empty DataFrames (matching PySpark behavior).
            engine_config: Engine configuration for validation and coercion.
            storage: Storage manager for the DataFrame.

        Returns:
            DataFrame instance with the specified data and schema.

        Raises:
            IllegalArgumentException: If data is not in the expected format.

        Example:
            >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            >>> df = factory.create_dataframe(data, None, config, storage)
            >>> import pandas as pd
            >>> pdf = pd.DataFrame({"name": ["Alice"], "age": [25]})
            >>> df = factory.create_dataframe(pdf, None, config, storage)

        Note:
            Fixed in version 3.23.0 (Issue #229): Pandas DataFrame recognition now uses
            duck typing to properly detect real pandas DataFrames even when mock pandas
            modules are present in the environment.
        """
        # Handle Pandas DataFrame
        # Use duck typing to detect pandas DataFrame (works for both mock and real pandas)
        # Check for to_dict method with orient parameter, which is characteristic of pandas DataFrame
        if hasattr(data, "to_dict") and callable(getattr(data, "to_dict", None)):
            try:
                # Try to call to_dict with orient="records" - this is the pandas DataFrame API
                test_dict = data.to_dict(orient="records")
                if isinstance(test_dict, list) and (
                    not test_dict or isinstance(test_dict[0], dict)
                ):
                    # This looks like a pandas DataFrame - convert it
                    data = test_dict
            except (TypeError, ValueError, AttributeError):
                # Not a pandas DataFrame or doesn't support orient="records"
                pass
        elif pd is not None and isinstance(data, pd.DataFrame):
            # Fallback: check against imported pandas (might be mock)
            data = data.to_dict(orient="records")

        if not isinstance(data, list):
            raise IllegalArgumentException(
                "Data must be a list of dictionaries, tuples, lists, Row objects, or a Pandas DataFrame"
            )

        # Handle PySpark StructType - convert to StructType
        # Check if it's a PySpark StructType (has 'fields' attribute but not StructType)
        if (
            schema is not None
            and not isinstance(schema, (StructType, str, list, DataType))
            and hasattr(schema, "fields")  # type: ignore[unreachable]
        ):
            # This is likely a PySpark StructType - convert it
            schema = self._convert_pyspark_struct_type(schema)  # type: ignore[unreachable]

        # Handle single DataType schema (PySpark compatibility)
        # Example: createDataFrame([date1, date2], DateType()).toDF("dates")
        # This must be checked BEFORE Row conversion because raw values are allowed here
        if (
            schema is not None
            and isinstance(schema, DataType)
            and not isinstance(schema, StructType)
        ):
            # Convert single DataType to StructType with a single unnamed field
            # Use "_c0" as the default column name (PySpark convention)
            single_data_type = schema
            schema = StructType([StructField("_c0", single_data_type, nullable=True)])

            # If data is positional (not dicts), convert to dicts with "_c0" key
            # This handles cases like createDataFrame([date1, date2], DateType())
            if data:

                def _is_positional_row(obj: Any) -> bool:
                    """Check if object is a positional row (sequence but not str/bytes/dict)."""
                    return isinstance(obj, Sequence) and not isinstance(
                        obj, (str, bytes, dict)
                    )

                # Check if first row is positional (primitive value or sequence)
                first_row = data[0]
                if not isinstance(first_row, dict):
                    # Convert positional data to dicts with "_c0" key
                    converted_data: List[Dict[str, Any]] = []
                    for row in data:
                        if isinstance(row, Row):
                            # Row objects are also allowed with single DataType schema
                            converted_data.append(
                                {
                                    "_c0": row.asDict().get("_c0")
                                    if "_c0" in row.asDict()
                                    else list(row.asDict().values())[0]
                                    if row.asDict()
                                    else None
                                }
                            )
                        elif _is_positional_row(row):
                            # Multi-value positional row (tuple/list with multiple values)
                            row_seq = cast("Sequence[Any]", row)
                            converted_data.append(
                                {"_c0": row_seq[0] if len(row_seq) > 0 else None}
                            )
                        else:
                            # Primitive value (single date, string, etc.)
                            converted_data.append({"_c0": row})
                    data = converted_data

        # Convert Row objects to dictionaries for consistent handling
        # This allows createDataFrame to accept Row objects (PySpark compatibility)
        # Only do this if we haven't already converted the data above
        if data and not all(isinstance(row, dict) for row in data):
            converted_data_list: List[
                Union[Dict[str, Any], Tuple[Any, ...], List[Any]]
            ] = []
            for row in data:
                if isinstance(row, Row):
                    # Convert Row object to dict using asDict()
                    converted_data_list.append(row.asDict())
                elif isinstance(row, (dict, tuple, list)):
                    # Keep as-is (lists and tuples are positional rows)
                    # These will be converted to dicts later by normalize_data_for_schema
                    converted_data_list.append(row)
                else:
                    # At this point, if we still have non-dict/tuple/list/Row values,
                    # it's an error (unless we already handled single DataType above)
                    raise IllegalArgumentException(
                        "Data must be a list of dictionaries, tuples, lists, or Row objects"
                    )
            data = converted_data_list

        # Handle DDL schema strings
        if isinstance(schema, str):
            from sparkless.core.ddl_adapter import parse_ddl_schema

            schema = parse_ddl_schema(schema)

        # Validate empty DataFrame schema requirements (PySpark compatibility)
        if not data:
            if schema is None:
                raise ValueError("can not infer schema from empty dataset")
            elif isinstance(schema, list):
                raise ValueError(
                    "can not infer schema from empty dataset. "
                    "Please provide a StructType schema instead of a column name list."
                )
            elif not isinstance(schema, StructType):
                raise TypeError(f"schema must be StructType, got {type(schema)}")
            # If schema is StructType, allow it (valid case)

        # Handle list of column names as schema (only for non-empty data)
        if isinstance(schema, list):
            # PySpark requires explicit schema for empty DataFrames
            if not data:
                raise ValueError("can not infer schema from empty dataset")

            def _is_positional_row(obj: Any) -> bool:
                """Check if object is a positional row (sequence but not str/bytes/dict)."""
                return isinstance(obj, Sequence) and not isinstance(
                    obj, (str, bytes, dict)
                )

            # Convert positional rows (list/tuple/etc.) to dicts using provided column names
            if data and _is_positional_row(data[0]):
                # IMPORTANT (BUG-57): When users provide a column-name list,
                # PySpark preserves that order in the resulting schema.
                # We therefore:
                #   1) Convert tuple rows into dicts keyed by the provided
                #      column names.
                #   2) Infer types column-by-column in *the provided order*
                #      instead of alphabetically (SchemaInferenceEngine
                #      sorts keys alphabetically by design).
                #   3) Normalize data to ensure all rows contain all columns.
                from sparkless.core.schema_inference import (
                    SchemaInferenceEngine,
                    normalize_data_for_schema,
                )

                reordered_data: List[Dict[str, Any]] = []
                column_names = schema
                for row in data:
                    if _is_positional_row(row):
                        # Type narrowing: row is a Sequence that supports indexing
                        row_seq = cast("Sequence[Any]", row)
                        row_dict = {
                            column_names[i]: row_seq[i]
                            for i in range(min(len(column_names), len(row_seq)))
                        }
                        reordered_data.append(row_dict)
                    else:
                        # row could be tuple/list that gets converted later
                        reordered_data.append(row)  # type: ignore[arg-type]
                data = reordered_data

                # Infer types without changing the user-provided column order
                fields: List[StructField] = []
                for name in column_names:
                    # Collect non-null values for this column
                    values_for_key = [
                        row[name]
                        for row in data
                        if isinstance(row, dict)
                        and name in row
                        and row[name] is not None
                    ]
                    if not values_for_key:
                        # Match SchemaInferenceEngine behavior for all-null columns
                        raise ValueError(
                            "Some of types cannot be determined after inferring"
                        )

                    field_type = SchemaInferenceEngine._infer_type(values_for_key[0])
                    # Check for type conflicts across rows (same logic as
                    # SchemaInferenceEngine.infer_from_data)
                    for value in values_for_key[1:]:
                        inferred_type = SchemaInferenceEngine._infer_type(value)
                        if type(field_type) is not type(inferred_type):
                            raise TypeError(
                                f"field {name}: Can not merge type "
                                f"{type(field_type).__name__} and "
                                f"{type(inferred_type).__name__}"
                            )

                    nullable = getattr(field_type, "nullable", True)
                    fields.append(StructField(name, field_type, nullable=nullable))

                schema = StructType(fields)
                # Normalize data so every row has every column in schema order
                data = normalize_data_for_schema(data, schema)
            else:
                # For non-tuple data with column names, infer types from actual data values
                # This matches PySpark behavior where column names don't force StringType
                from sparkless.core.schema_inference import (
                    SchemaInferenceEngine,
                    normalize_data_for_schema,
                )

                inferred_fields: List[StructField] = []
                for name in schema:
                    # Collect non-null values for this column
                    values_for_key = [
                        row[name]
                        for row in data
                        if isinstance(row, dict)
                        and name in row
                        and row[name] is not None
                    ]
                    if not values_for_key:
                        # Match SchemaInferenceEngine behavior for all-null columns
                        raise ValueError(
                            "Some of types cannot be determined after inferring"
                        )

                    field_type = SchemaInferenceEngine._infer_type(values_for_key[0])
                    # Check for type conflicts across rows (same logic as
                    # SchemaInferenceEngine.infer_from_data)
                    for value in values_for_key[1:]:
                        inferred_type = SchemaInferenceEngine._infer_type(value)
                        if type(field_type) is not type(inferred_type):
                            raise TypeError(
                                f"field {name}: Can not merge type "
                                f"{type(field_type).__name__} and "
                                f"{type(inferred_type).__name__}"
                            )

                    nullable = getattr(field_type, "nullable", True)
                    inferred_fields.append(
                        StructField(name, field_type, nullable=nullable)
                    )

                schema = StructType(inferred_fields)
                # Normalize data so every row has every column in schema order
                data = normalize_data_for_schema(data, schema)  # type: ignore[arg-type]

        if schema is None:
            # Infer schema from data using SchemaInferenceEngine
            # Note: Empty data case is already handled above
            if not data:
                # This should not happen due to validation above, but keep as safety
                raise ValueError("can not infer schema from empty dataset")
            else:
                # Check if data is in expected format
                # Note: Row objects have already been converted to dicts above
                sample_row = data[0] if data else None

                if sample_row is None:
                    raise ValueError("can not infer schema from empty dataset")

                if isinstance(sample_row, dict):
                    # Use SchemaInferenceEngine for dictionary data
                    from sparkless.core.schema_inference import SchemaInferenceEngine

                    schema, data = SchemaInferenceEngine.infer_from_data(data)  # type: ignore[arg-type]
                elif isinstance(sample_row, tuple):
                    # For tuples, we need column names - this should have been handled earlier
                    # If we get here, it's an error
                    raise IllegalArgumentException(
                        "Cannot infer schema from tuples without column names. "
                        "Please provide schema or use list of column names."
                    )

        # Apply validation and optional type coercion per mode
        # Note: When an explicit schema is provided with empty data, we still need to preserve
        # the schema even though validation is skipped (since there's no data to validate)
        if isinstance(schema, StructType) and data:
            # Convert tuple-based data to dictionaries when StructType schema is provided
            # This ensures compatibility with all DataFrame operations that expect dict rows
            # (Issue #270: Fix for tuple-based data parameter)
            def _is_positional_row(obj: Any) -> bool:
                """Check if object is a positional row (sequence but not str/bytes/dict)."""
                return isinstance(obj, Sequence) and not isinstance(
                    obj, (str, bytes, dict, Row)
                )

            # Check if any rows are positional (tuple/list)
            if any(_is_positional_row(row) for row in data):
                field_names = [field.name for field in schema.fields]
                field_count = len(field_names)

                # PySpark requires strict length matching - validate before conversion
                for i, row in enumerate(data):
                    if _is_positional_row(row):
                        row_seq = cast("Sequence[Any]", row)
                        if len(row_seq) != field_count:
                            raise IllegalArgumentException(
                                f"LENGTH_SHOULD_BE_THE_SAME: obj and fields should be "
                                f"of the same length, got {len(row_seq)} and {field_count}. "
                                f"Row {i} has {len(row_seq)} elements but schema has {field_count} fields."
                            )

                tuple_to_dict_data: List[Dict[str, Any]] = []

                for row in data:
                    if isinstance(row, Row):
                        # Row objects already handled earlier, convert to dict
                        tuple_to_dict_data.append(row.asDict())
                    elif _is_positional_row(row):
                        # Convert tuple/list to dict using schema field names
                        # At this point we know lengths match due to validation above
                        row_seq = cast("Sequence[Any]", row)
                        row_dict = {
                            field_names[i]: row_seq[i] for i in range(field_count)
                        }
                        tuple_to_dict_data.append(row_dict)
                    else:
                        # Already a dict, keep as-is
                        tuple_to_dict_data.append(cast("Dict[str, Any]", row))

                data = tuple_to_dict_data

            from sparkless.core.data_validation import DataValidator

            validator = DataValidator(
                schema,
                validation_mode=engine_config.validation_mode,
                enable_coercion=engine_config.enable_type_coercion,
            )

            # Validate if in strict mode
            if engine_config.validation_mode == "strict":
                validator.validate(data)  # type: ignore[arg-type]

            # Coerce if enabled
            if engine_config.enable_type_coercion:
                data = validator.coerce(data)  # type: ignore[arg-type]

        # Ensure schema is always StructType at this point
        # IMPORTANT: When explicit schema is provided with empty data, preserve it!
        # Note: At this point, schema should always be StructType due to all previous
        # transformations, but we check for safety and to satisfy type checking
        if not isinstance(schema, StructType):
            # This should never happen, but provide a fallback
            schema = StructType([])

        # Validate that schema is properly initialized with fields attribute
        # This ensures empty DataFrames with explicit schemas preserve column information
        # Note: After the above check, schema is guaranteed to be StructType
        if not hasattr(schema, "fields"):
            # This shouldn't happen, but handle edge case
            schema = StructType([])
            # fields can be empty list, but that's valid for empty schemas
            # If schema was provided explicitly, trust it even if fields is empty

        return DataFrame(data, schema, storage)  # type: ignore[arg-type]

    def _handle_schema_inference(
        self, data: List[Dict[str, Any]], schema: Optional[Any]
    ) -> Tuple[StructType, List[Dict[str, Any]]]:
        """Handle schema inference or conversion.

        Args:
            data: List of dictionaries representing rows.
            schema: Optional schema definition.

        Returns:
            Tuple of (inferred_schema, normalized_data).
        """
        if schema is None:
            from sparkless.core.schema_inference import SchemaInferenceEngine

            return SchemaInferenceEngine.infer_from_data(data)
        else:
            # Schema provided, return as-is
            # Schema should be StructType at this point, but ensure it's typed correctly
            return cast("StructType", schema), data

    def _apply_validation_and_coercion(
        self,
        data: List[Dict[str, Any]],
        schema: StructType,
        engine_config: SparkConfig,
    ) -> List[Dict[str, Any]]:
        """Apply validation and type coercion.

        Args:
            data: List of dictionaries representing rows.
            schema: Schema to validate against.
            engine_config: Engine configuration.

        Returns:
            Validated and coerced data.
        """
        from sparkless.core.data_validation import DataValidator

        validator = DataValidator(
            schema,
            validation_mode=engine_config.validation_mode,
            enable_coercion=engine_config.enable_type_coercion,
        )

        # Validate if in strict mode
        if engine_config.validation_mode == "strict":
            validator.validate(data)

        # Coerce if enabled
        if engine_config.enable_type_coercion:
            data = validator.coerce(data)

        return data

    def _convert_pyspark_struct_type(self, pyspark_schema: Any) -> StructType:
        """Convert PySpark StructType to StructType.

        Args:
            pyspark_schema: PySpark StructType object with 'fields' attribute.

        Returns:
            StructType equivalent.
        """
        from sparkless.spark_types import (
            DataType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            BooleanType,
            TimestampType,
            DateType,
            DecimalType,
            ArrayType,
            MapType,
            StructType,
        )

        def convert_pyspark_field(field: Any) -> StructField:
            """Convert PySpark StructField to StructField."""
            field_name = field.name
            field_nullable = getattr(field, "nullable", True)

            # Convert PySpark data type to Sparkless data type
            pyspark_type = field.dataType
            mock_type = convert_pyspark_data_type(pyspark_type)

            return StructField(
                name=field_name, dataType=mock_type, nullable=field_nullable
            )

        def convert_pyspark_data_type(pyspark_type: Any) -> DataType:
            """Convert PySpark DataType to DataType."""
            # Get the type name as string for comparison
            type_name = type(pyspark_type).__name__

            if type_name == "StringType":
                return StringType()
            elif type_name == "IntegerType":
                return IntegerType()
            elif type_name == "LongType":
                return LongType()
            elif type_name == "FloatType":
                return FloatType()
            elif type_name == "DoubleType":
                return DoubleType()
            elif type_name == "BooleanType":
                return BooleanType()
            elif type_name == "TimestampType":
                return TimestampType()
            elif type_name == "DateType":
                return DateType()
            elif type_name == "DecimalType":
                precision = getattr(pyspark_type, "precision", 10)
                scale = getattr(pyspark_type, "scale", 0)
                return DecimalType(precision=precision, scale=scale)
            elif type_name == "ArrayType":
                element_type = convert_pyspark_data_type(pyspark_type.elementType)
                return ArrayType(element_type)
            elif type_name == "MapType":
                key_type = convert_pyspark_data_type(pyspark_type.keyType)
                value_type = convert_pyspark_data_type(pyspark_type.valueType)
                return MapType(key_type, value_type)
            elif type_name == "StructType":
                # Recursive conversion for nested structs
                fields = [convert_pyspark_field(f) for f in pyspark_type.fields]
                return StructType(fields)
            else:
                # Default to StringType for unknown types
                return StringType()

        # Convert all fields
        fields = [convert_pyspark_field(field) for field in pyspark_schema.fields]
        return StructType(fields)

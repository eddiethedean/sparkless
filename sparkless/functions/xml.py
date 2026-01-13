"""XML functions for PySpark 3.2+ compatibility."""

from typing import Union, cast
from sparkless.functions.core.column import Column
from sparkless.functions.base import ColumnOperation


class XMLFunctions:
    """XML parsing and manipulation functions."""

    @staticmethod
    def from_xml(col: Union[Column, str], schema: str) -> ColumnOperation:
        """Parse XML string to struct based on schema.

        Args:
            col: Column containing XML strings.
            schema: Schema definition string.

        Returns:
            ColumnOperation representing the from_xml function.

        Example:
            >>> df.select(F.from_xml(F.col("xml"), "name STRING, age INT"))
        """
        if isinstance(col, str):
            col = Column(col)

        return ColumnOperation(
            col,
            "from_xml",
            schema,
            name=f"from_xml({col.name}, '{schema}')",
        )

    @staticmethod
    def to_xml(
        col: Union[Column, ColumnOperation, str],
    ) -> ColumnOperation:  # str may be passed at runtime
        """Convert struct column to XML string.

        Args:
            col: Struct column to convert.

        Returns:
            ColumnOperation representing the to_xml function.

        Example:
            >>> df.select(F.to_xml(F.struct(F.col("name"), F.col("age"))))
        """
        if isinstance(col, str):
            col_obj: Union[Column, ColumnOperation] = Column(col)
        elif isinstance(col, (Column, ColumnOperation)):
            col_obj = col
        else:
            # For other types, convert to Column
            col_obj = Column(str(col))  # type: ignore[unreachable]

        if isinstance(col_obj, Column):
            base_col = col_obj
        else:
            # At this point, col_obj must be ColumnOperation
            base_col = cast("ColumnOperation", col_obj).column  # type: ignore[unreachable]

        return ColumnOperation(
            base_col,
            "to_xml",
            col if isinstance(col, ColumnOperation) else None,
            name=f"to_xml({getattr(col, 'name', 'struct')})",
        )

    @staticmethod
    def schema_of_xml(col: Union[Column, str]) -> ColumnOperation:
        """Infer schema from XML string.

        Args:
            col: Column containing XML strings.

        Returns:
            ColumnOperation representing the schema_of_xml function.

        Example:
            >>> df.select(F.schema_of_xml(F.col("xml")))
        """
        if isinstance(col, str):
            col = Column(col)

        return ColumnOperation(
            col,
            "schema_of_xml",
            name=f"schema_of_xml({col.name})",
        )

    @staticmethod
    def xpath(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract array of values from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath function.

        Example:
            >>> df.select(F.xpath(F.col("xml"), "/root/item"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath",
            path,
            name=f"xpath({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_boolean(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Evaluate XPath expression to boolean.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_boolean function.

        Example:
            >>> df.select(F.xpath_boolean(F.col("xml"), "/root/active='true'"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_boolean",
            path,
            name=f"xpath_boolean({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_double(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract double value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_double function.

        Example:
            >>> df.select(F.xpath_double(F.col("xml"), "/root/value"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_double",
            path,
            name=f"xpath_double({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_float(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract float value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_float function.

        Example:
            >>> df.select(F.xpath_float(F.col("xml"), "/root/price"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_float",
            path,
            name=f"xpath_float({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_int(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract integer value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_int function.

        Example:
            >>> df.select(F.xpath_int(F.col("xml"), "/root/age"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_int",
            path,
            name=f"xpath_int({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_long(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract long value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_long function.

        Example:
            >>> df.select(F.xpath_long(F.col("xml"), "/root/value"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_long",
            path,
            name=f"xpath_long({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_short(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract short value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_short function.

        Example:
            >>> df.select(F.xpath_short(F.col("xml"), "/root/count"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_short",
            path,
            name=f"xpath_short({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_string(xml: Union[Column, str], path: str) -> ColumnOperation:
        """Extract string value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            ColumnOperation representing the xpath_string function.

        Example:
            >>> df.select(F.xpath_string(F.col("xml"), "/root/name"))
        """
        if isinstance(xml, str):
            xml = Column(xml)

        return ColumnOperation(
            xml,
            "xpath_string",
            path,
            name=f"xpath_string({xml.name}, '{path}')",
        )

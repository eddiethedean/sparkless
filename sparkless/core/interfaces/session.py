"""
Session interface definitions.

This module defines the abstract interfaces for session management,
SQL processing, and catalog operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from ..interfaces.dataframe import IDataFrame, IDataFrameReader


class ISession(ABC):
    """Abstract interface for Spark session operations."""

    @property
    @abstractmethod
    def appName(self) -> str:
        """Get application name."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Get Spark version."""
        pass

    @property
    @abstractmethod
    def sparkContext(self) -> "ISparkContext":
        """Get Spark context."""
        pass

    @property
    @abstractmethod
    def catalog(self) -> "ICatalog":
        """Get catalog."""
        pass

    @property
    @abstractmethod
    def conf(self) -> "IConfiguration":
        """Get configuration."""
        pass

    @property
    @abstractmethod
    def read(self) -> IDataFrameReader:
        """Get DataFrame reader."""
        pass

    @abstractmethod
    def createDataFrame(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
        schema: Optional[Union[str, Any]] = None,
    ) -> IDataFrame:
        """Create DataFrame from data."""
        pass

    @abstractmethod
    def sql(self, query: str) -> IDataFrame:
        """Execute SQL query."""
        pass

    @abstractmethod
    def table(self, table_name: str) -> IDataFrame:
        """Get table as DataFrame."""
        pass

    @abstractmethod
    def range(
        self, start: int, end: int, step: int = 1, numPartitions: Optional[int] = None
    ) -> IDataFrame:
        """Create DataFrame with range of numbers."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the session."""
        pass

    @abstractmethod
    def newSession(self) -> "ISession":
        """Create new session."""
        pass


class ISparkContext(ABC):
    """Abstract interface for Spark context operations."""

    @property
    @abstractmethod
    def appName(self) -> str:
        """Get application name."""
        pass

    @abstractmethod
    def setLogLevel(self, level: str) -> None:
        """Set log level."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the context."""
        pass


class ICatalog(ABC):
    """Abstract interface for catalog operations."""

    @abstractmethod
    def listDatabases(self) -> List[Any]:
        """List all databases."""
        pass

    @abstractmethod
    def listTables(self, dbName: Optional[str] = None) -> List[Any]:
        """List tables in database."""
        pass

    @abstractmethod
    def createDatabase(self, db_name: str, **options: Any) -> None:
        """Create database."""
        pass

    @abstractmethod
    def dropDatabase(
        self,
        db_name: str,
        ignoreIfNotExists: bool = True,
        ignore_if_not_exists: Optional[bool] = None,
        cascade: bool = False,
    ) -> None:
        """Drop database."""
        pass

    @abstractmethod
    def createTable(self, table_name: str, **options: Any) -> None:
        """Create table."""
        pass

    @abstractmethod
    def dropTable(self, table_name: str, ignore_if_not_exists: bool = False) -> None:
        """Drop table."""
        pass

    @abstractmethod
    def tableExists(self, table_name: str, db_name: Optional[str] = None) -> bool:
        """Check if table exists."""
        pass

    @abstractmethod
    def databaseExists(self, db_name: str) -> bool:
        """Check if database exists."""
        pass

    @abstractmethod
    def getDatabase(self, db_name: str) -> Any:
        """Get database information."""
        pass

    @abstractmethod
    def getTable(self, table_name: str, db_name: Optional[str] = None) -> Any:
        """Get table information."""
        pass

    @abstractmethod
    def currentDatabase(self) -> str:
        """Get current database name."""
        pass

    @abstractmethod
    def get_storage_backend(self) -> Any:
        """Get the storage backend instance."""
        pass


class IConfiguration(ABC):
    """Abstract interface for configuration management."""

    @abstractmethod
    def get(self, key: str, default_value: Optional[str] = None) -> Optional[str]:
        """Get configuration value."""
        pass

    @abstractmethod
    def set(self, key: str, value: str) -> None:
        """Set configuration value."""
        pass

    @abstractmethod
    def getAll(self) -> Dict[str, str]:
        """Get all configuration values."""
        pass

    @abstractmethod
    def unset(self, key: str) -> None:
        """Unset configuration value."""
        pass


class ISQLProcessor(ABC):
    """Abstract interface for SQL processing."""

    @abstractmethod
    def parse(self, query: str) -> Any:
        """Parse SQL query."""
        pass

    @abstractmethod
    def execute(self, query: str) -> IDataFrame:
        """Execute SQL query."""
        pass

    @abstractmethod
    def optimize(self, query: Any) -> Any:
        """Optimize SQL query."""
        pass

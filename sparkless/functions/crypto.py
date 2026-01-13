"""
Cryptographic functions for Sparkless.

This module provides cryptographic functions that match PySpark's crypto function API.
Includes encryption and decryption operations for secure data processing in DataFrames.

Key Features:
    - AES encryption and decryption
    - Null-safe cryptographic operations
    - Type-safe operations with proper return types
    - Support for both column references and string literals

Example:
    >>> from sparkless.sql import SparkSession, functions as F
    >>> spark = SparkSession("test")
    >>> data = [{"data": "sensitive information", "key": "secretkey"}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.aes_encrypt(F.col("data"), F.col("key")),
    ...     F.aes_decrypt(F.col("encrypted"), F.col("key"))
    ... ).show()
"""

from typing import Optional, Union
from sparkless.functions.base import Column, ColumnOperation


class CryptoFunctions:
    """Collection of cryptographic functions."""

    @staticmethod
    def aes_encrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Encrypt data using AES encryption.

        Args:
            data: The column containing data to encrypt.
            key: The column containing the encryption key.
            mode: Encryption mode (optional, defaults to GCM).
            padding: Padding scheme (optional, defaults to PKCS5).

        Returns:
            ColumnOperation representing the aes_encrypt function.
        """
        if isinstance(data, str):
            data = Column(data)
        if isinstance(key, str):
            key = Column(key)

        # Build optional parameters
        params = []
        if mode is not None:
            params.append(f"mode={mode}")
        if padding is not None:
            params.append(f"padding={padding}")

        param_str = ", " + ", ".join(params) if params else ""

        # Store mode and padding in value tuple
        value_tuple = (key, mode, padding) if mode or padding else key
        operation = ColumnOperation(
            data,
            "aes_encrypt",
            value=value_tuple,
            name=f"aes_encrypt({data.name}, {key.name}{param_str})",
        )
        return operation

    @staticmethod
    def aes_decrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Decrypt data using AES decryption.

        Args:
            data: The column containing encrypted data.
            key: The column containing the decryption key.
            mode: Decryption mode (optional, defaults to GCM).
            padding: Padding scheme (optional, defaults to PKCS5).

        Returns:
            ColumnOperation representing the aes_decrypt function.
        """
        if isinstance(data, str):
            data = Column(data)
        if isinstance(key, str):
            key = Column(key)

        # Build optional parameters
        params = []
        if mode is not None:
            params.append(f"mode={mode}")
        if padding is not None:
            params.append(f"padding={padding}")

        param_str = ", " + ", ".join(params) if params else ""

        # Store mode and padding in value tuple
        value_tuple = (key, mode, padding) if mode or padding else key
        operation = ColumnOperation(
            data,
            "aes_decrypt",
            value=value_tuple,
            name=f"aes_decrypt({data.name}, {key.name}{param_str})",
        )
        return operation

    @staticmethod
    def try_aes_decrypt(
        data: Union[Column, str],
        key: Union[Column, str],
        mode: Optional[str] = None,
        padding: Optional[str] = None,
    ) -> ColumnOperation:
        """Null-safe AES decryption - returns NULL on error instead of throwing exception.

        Args:
            data: The column containing encrypted data.
            key: The column containing the decryption key.
            mode: Decryption mode (optional, defaults to GCM).
            padding: Padding scheme (optional, defaults to PKCS5).

        Returns:
            ColumnOperation representing the try_aes_decrypt function.
        """
        if isinstance(data, str):
            data = Column(data)
        if isinstance(key, str):
            key = Column(key)

        # Build optional parameters
        params = []
        if mode is not None:
            params.append(f"mode={mode}")
        if padding is not None:
            params.append(f"padding={padding}")

        param_str = ", " + ", ".join(params) if params else ""

        # Store mode and padding in value tuple
        value_tuple = (key, mode, padding) if mode or padding else key
        operation = ColumnOperation(
            data,
            "try_aes_decrypt",
            value=value_tuple,
            name=f"try_aes_decrypt({data.name}, {key.name}{param_str})",
        )
        return operation

"""
Service for coordinating mocking and error simulation.

This module provides the MockingCoordinator class, which handles
mocking of methods, error simulation rules, and test coordination.
"""

from typing import Any, Dict, List, Optional, Tuple


class MockingCoordinator:
    """
    Coordinates mocking and error simulation for testing.

    Handles method mocking, error rule management, and test coordination
    to provide a clean testing interface for SparkSession.
    """

    def __init__(self) -> None:
        """Initialize the mocking coordinator."""
        self._error_rules: Dict[str, List[Tuple[Any, Exception]]] = {}

    def setup_mock_impl(
        self,
        method_name: str,
        side_effect: Optional[Exception] = None,
        return_value: Optional[Any] = None,
    ) -> Any:
        """Set up a mock implementation for a method.

        Args:
            method_name: Name of the method to mock.
            side_effect: Exception to raise when method is called.
            return_value: Value to return when method is called.

        Returns:
            Mock implementation function.
        """
        if side_effect is not None:
            exception_to_raise = side_effect

            def mock_impl(*args: Any, **kwargs: Any) -> Any:
                raise exception_to_raise

            return mock_impl
        if return_value is not None:

            def mock_impl(*args: Any, **kwargs: Any) -> Any:
                return return_value

            return mock_impl
        else:
            # Default mock that returns None
            def mock_impl(*args: Any, **kwargs: Any) -> Any:
                return None

            return mock_impl

    def reset_all_mocks(self, original_impls: Dict[str, Any]) -> Dict[str, Any]:
        """Reset all mocks to original implementations.

        Args:
            original_impls: Dictionary of original implementations.

        Returns:
            Dictionary of original implementations.
        """
        # Clear error rules
        self.clear_error_rules()

        # Return original implementations for reset
        return original_impls

    def add_error_rule(
        self, method_name: str, condition: Any, exception: Exception
    ) -> None:
        """Add an error simulation rule.

        Args:
            method_name: Name of the method to add error rule for.
            condition: Condition function that determines when to raise error.
            exception: Exception to raise when condition is met.
        """
        if method_name not in self._error_rules:
            self._error_rules[method_name] = []
        self._error_rules[method_name].append((condition, exception))

    def check_error_rules(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Optional[Exception]:
        """Check if error should be raised for method.

        Args:
            method_name: Name of the method being called.
            *args: Positional arguments passed to method.
            **kwargs: Keyword arguments passed to method.

        Returns:
            Exception to raise if error rule matches, None otherwise.
        """
        if method_name in self._error_rules:
            for condition, exception in self._error_rules[method_name]:
                if condition(*args, **kwargs):
                    return exception
        return None

    def clear_error_rules(self) -> None:
        """Clear all error simulation rules."""
        self._error_rules.clear()

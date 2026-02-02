Welcome to Sparkless
====================

**🚀 Test PySpark code at lightning speed—no JVM required**

Sparkless is a lightweight PySpark replacement that runs your tests **10x faster** by eliminating JVM overhead. Your existing PySpark code works unchanged—just swap the import.

.. code-block:: python

   # Before
   from pyspark.sql import SparkSession

   # After
   from sparkless.sql import SparkSession

Key Features
------------

* ⚡ **10x Faster** - No JVM startup (30s → 0.1s)
* 🎯 **Drop-in Replacement** - Use existing PySpark code unchanged
* 📦 **Zero Java** - Pure Python with Polars backend (thread-safe, no SQL required)
* 🧪 **100% Compatible** - Full PySpark 3.2-3.5 API support
* 🔄 **Lazy Evaluation** - Mirrors PySpark's execution model
* 🏭 **Production Ready** - 2314+ passing tests, 100% mypy typed
* 🧵 **Thread-Safe** - Polars backend designed for parallel execution

Quick Start
-----------

.. code-block:: python

   from sparkless.sql import SparkSession, functions as F

   # Create session
   spark = SparkSession("MyApp")

   # Your PySpark code works as-is
   data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
   df = spark.createDataFrame(data)

   # All operations work
   result = df.filter(F.col("age") > 25).select("name").collect()
   print(result)
   # Output: [Row(name='Bob')]

Documentation Contents
----------------------

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started
   installation
   api_reference

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index
   api/session
   api/dataframe
   api/functions
   api/types
   api/backend
   api/storage

.. toctree::
   :maxdepth: 2
   :caption: Guides

   guides/index
   guides/migration
   guides/configuration
   guides/lazy_evaluation
   guides/cte_optimization
   guides/pytest_integration
   guides/benchmarking
   guides/memory_management
   guides/threading
   guides/plugins

.. toctree::
   :maxdepth: 1
   :caption: Advanced Topics

   backend_architecture
   backend_selection
   sql_operations_guide
   storage_api_guide
   storage_serialization_guide
   testing_patterns
   performance/profiling
   performance/pandas_fallback

.. toctree::
   :maxdepth: 1
   :caption: Additional Resources

   known_issues
   migration_from_pyspark
   migration_from_v2_to_v3
   mock_spark_features
   function_api_audit

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

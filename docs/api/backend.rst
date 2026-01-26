Backend Architecture
====================

Sparkless uses a modular backend architecture that allows different execution engines.

Backend Protocols
-----------------

.. automodule:: sparkless.backend.protocols
   :members:
   :undoc-members:
   :show-inheritance:

Polars Backend
--------------

The default backend uses Polars for high-performance DataFrame operations.

.. automodule:: sparkless.backend.polars.operation_executor
   :members:
   :undoc-members:

.. automodule:: sparkless.backend.polars.materializer
   :members:
   :undoc-members:

.. automodule:: sparkless.backend.polars.expression_translator
   :members:
   :undoc-members:

.. automodule:: sparkless.backend.polars.storage
   :members:
   :undoc-members:

Storage Backends
----------------

.. automodule:: sparkless.storage.manager
   :members:
   :undoc-members:

.. automodule:: sparkless.storage.backends.memory
   :members:
   :undoc-members:

.. automodule:: sparkless.storage.backends.file
   :members:
   :undoc-members:

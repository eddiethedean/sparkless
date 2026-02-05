# Pytest Integration

Built-in patterns for using Sparkless in pytest suites.

```python
# conftest.py
import pytest
from sparkless.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession(validation_mode="relaxed")
```

```python
# example test
from sparkless import functions as F

def test_basic(spark):
    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    assert df.filter(F.col("x") > 1).count() == 1
```

## Running tests with the Robin backend

To run the suite using the Robin (robin-sparkless) backend, set the environment before invoking pytest or the test script:

```bash
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest tests/ -v
# or
SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh
```

Install the optional dependency first: `pip install sparkless[robin]` or `pip install robin-sparkless`. If Robin is selected but not installed, tests that use the `spark` fixture are skipped with a clear message.


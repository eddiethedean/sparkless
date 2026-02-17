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

## Running tests (v4)

In v4, Sparkless uses the Robin engine only. To run the test suite (with the `spark` fixture creating a Robin session):

```bash
SPARKLESS_TEST_BACKEND=robin pytest tests/ -v --ignore=tests/archive
# or
SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh
```

For parallel runs:

```bash
SPARKLESS_TEST_BACKEND=robin pytest tests/ -n 12 -v --ignore=tests/archive
```

The Robin extension is built into Sparkless (no separate `pip install robin-sparkless`). Source builds require Rust and maturin.


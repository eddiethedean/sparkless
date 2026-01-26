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


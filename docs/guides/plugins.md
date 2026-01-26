# Plugins


Register plugins to hook into DataFrame creation and operations.

```python
from sparkless.sql import SparkSession

class AuditPlugin:
    def before_create_dataframe(self, session, data, schema):
        # mutate data or schema
        return data, schema

    def after_create_dataframe(self, session, df):
        # inspect or wrap df
        return df

spark = SparkSession()
spark._register_plugin(AuditPlugin())

_ = spark.createDataFrame([{"id": 1}], ["id"])  # hooks will run
```

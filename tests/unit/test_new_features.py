"""Tests for new features: unpersist, bucketBy/sortBy, StorageLevel,
dropTempView/dropGlobalTempView, Catalog fields, df.stat, withMetadata,
insertInto.
"""

import pytest
from sparkless import SparkSession, StorageLevel


@pytest.fixture
def spark():
    session = SparkSession("test_new_features")
    yield session
    session.stop()


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    )


# ---- 1. DataFrame.unpersist() ----


class TestUnpersist:
    def test_unpersist_returns_self(self, df):
        result = df.cache().unpersist()
        assert result is not None

    def test_unpersist_clears_cached_flag(self, df):
        df.cache()
        assert df._is_cached is True
        df.unpersist()
        assert df._is_cached is False

    def test_unpersist_with_blocking(self, df):
        result = df.cache().unpersist(blocking=True)
        assert result is not None


# ---- 2. bucketBy / sortBy ----


class TestBucketBySortBy:
    def test_bucketBy_returns_writer(self, df):
        writer = df.write
        result = writer.bucketBy(10, "name")
        assert result is writer

    def test_sortBy_returns_writer(self, df):
        writer = df.write
        result = writer.sortBy("name")
        assert result is writer

    def test_chaining(self, df):
        writer = df.write
        result = writer.bucketBy(10, "name", "age").sortBy("age")
        assert result is writer


# ---- 3. StorageLevel ----


class TestStorageLevel:
    def test_constants_exist(self):
        assert StorageLevel.MEMORY_ONLY == "MEMORY_ONLY"
        assert StorageLevel.DISK_ONLY == "DISK_ONLY"
        assert StorageLevel.MEMORY_AND_DISK == "MEMORY_AND_DISK"
        assert StorageLevel.NONE == "NONE"
        assert StorageLevel.OFF_HEAP == "OFF_HEAP"

    def test_pyspark_module_alias(self):
        import sys

        # sparkless registers pyspark.storagelevel in sys.modules on import
        assert "pyspark.storagelevel" in sys.modules
        sl = sys.modules["pyspark.storagelevel"]
        assert sl.StorageLevel is StorageLevel


# ---- 4. dropTempView / dropGlobalTempView ----


class TestDropTempView:
    def test_drop_existing_temp_view(self, spark, df):
        df.createOrReplaceTempView("my_view")
        result = spark.catalog.dropTempView("my_view")
        assert result is True

    def test_drop_nonexistent_temp_view(self, spark):
        result = spark.catalog.dropTempView("nonexistent_view")
        assert result is False

    def test_drop_existing_global_temp_view(self, spark, df):
        df.createGlobalTempView("my_global_view")
        result = spark.catalog.dropGlobalTempView("my_global_view")
        assert result is True

    def test_drop_nonexistent_global_temp_view(self, spark):
        result = spark.catalog.dropGlobalTempView("nonexistent_global_view")
        assert result is False


# ---- 5. Catalog fields ----


class TestCatalogFields:
    def test_database_fields(self, spark):
        from sparkless.session.catalog import Database

        db = Database("test_db")
        assert db.catalog == "spark_catalog"
        assert db.description == ""
        assert db.locationUri == ""

    def test_table_fields(self, spark):
        from sparkless.session.catalog import Table

        t = Table("test_table")
        assert t.catalog == "spark_catalog"
        assert t.description == ""
        assert t.tableType == "MANAGED"
        assert t.isTemporary is False
        assert t.locationUri == ""


# ---- 6. df.stat proxy ----


class TestStatProxy:
    def test_stat_returns_stat_functions(self, df):
        from sparkless.dataframe.dataframe import DataFrameStatFunctions

        assert isinstance(df.stat, DataFrameStatFunctions)

    def test_stat_corr(self, df):
        result = df.stat.corr("age", "age")
        assert result == 0.0

    def test_stat_cov(self, df):
        result = df.stat.cov("age", "age")
        assert result == 0.0

    def test_stat_approxQuantile(self, df):
        result = df.stat.approxQuantile("age", [0.5], 0.01)
        assert isinstance(result, list)


# ---- 7. withMetadata ----


class TestWithMetadata:
    def test_withMetadata_returns_dataframe(self, df):
        result = df.withMetadata("name", {"comment": "person name"})
        assert result is not None

    def test_withMetadata_invalid_column(self, df):
        from sparkless.errors import AnalysisException

        with pytest.raises(AnalysisException):
            df.withMetadata("nonexistent", {"comment": "test"})


# ---- 8. insertInto ----


class TestInsertInto:
    def test_insertInto_append(self, spark, df):
        df.write.mode("overwrite").saveAsTable("insert_target")
        df2 = spark.createDataFrame([{"name": "Charlie", "age": 35}])
        df2.write.insertInto("insert_target")
        result = spark.table("insert_target")
        assert result.count() == 3

    def test_insertInto_overwrite(self, spark, df):
        df.write.mode("overwrite").saveAsTable("insert_target_ow")
        df2 = spark.createDataFrame([{"name": "Charlie", "age": 35}])
        df2.write.insertInto("insert_target_ow", overwrite=True)
        result = spark.table("insert_target_ow")
        assert result.count() == 1

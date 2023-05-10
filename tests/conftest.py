from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope='session')
def spark_session() -> SparkSession:
    yield SparkSession.builder.master("local").appName("test_session").getOrCreate()

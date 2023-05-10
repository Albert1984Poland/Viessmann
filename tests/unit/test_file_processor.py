from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType
import pytest
from viessmann_lib.file_processor import FileProcessor
from pandas.testing import assert_frame_equal

TEST_SELECTION_SCHEMA = StructType(
    fields=[
        StructField("dt", DateType()),
        StructField("AverageTemperature", DoubleType()),
        StructField("AverageTemperatureUncertainty", DoubleType()),
        StructField("State", StringType()),
        StructField("Country", StringType())
    ]
)


@pytest.fixture(scope='class', autouse=True)
def file_processor(spark_session: SparkSession) -> FileProcessor:
    return FileProcessor(src_path='./tests/data/avg_temp_test.csv',
                         dst_path='',
                         spark_session=spark_session)


class TestFileProcessor:

    def test_file_schema(self, file_processor: FileProcessor, spark_session: SparkSession):
        file_processor.read_file()
        assert spark_session.table('table').schema == TEST_SELECTION_SCHEMA

    def test_calculate_avg(self, file_processor: FileProcessor, spark_session: SparkSession):
        schema = ['Country', 'State', 'Max_Temp']
        data = [
            ('Brazil', None, 26.06),
            ('Brazil', 'Minas Gerais', 23.87),
            ('Brazil', 'Mato Grosso', 26.06),
            ('Brazil', 'Distrito Federal', 22.05),
            ('Brazil', 'Acre', 25.68)
        ]
        expected_df = spark_session.createDataFrame(data=data, schema=schema).toPandas()
        actual_df = file_processor.calculate_avg().toPandas()
        assert_frame_equal(left=expected_df,
                           right=actual_df)

from pyspark.sql import DataFrame, session
from pyspark.sql.types import DateType, DoubleType, StringType, StructType, StructField


class FileProcessor:
    SCHEMA = StructType(fields=[StructField("dt", DateType(), True),
                                StructField("AverageTemperature", DoubleType(), True),
                                StructField("AverageTemperatureUncertainty", DoubleType(), True),
                                StructField("State", StringType(), True),
                                StructField("Country", StringType(), True)]
                        )

    def __init__(self, src_path: str,
                 dst_path: str,
                 spark_session: session.SparkSession
                 ):
        self.src_path = src_path
        self.dst_path = dst_path
        self.spark = spark_session

    def read_file(self) -> None:
        """
        Method responsible converting csv file to DataFrame and registering it as a view.
        """

        df = self.spark.read.schema(self.SCHEMA).csv(self.src_path, header=True)
        df.createOrReplaceTempView('table')

    def calculate_avg(self) -> DataFrame:
        """
        Method responsible for calculations.
        """
        sql_query = """
                SELECT
                    Country,
                    State,
                    ROUND(MAX(AverageTemperature),2) as Max_Temp
                FROM table
                GROUP BY
                GROUPING SETS ((Country), (Country, State) )
            """

        return self.spark.sql(sql_query)

    def save_file(self, df: DataFrame) -> None:
        df.write.format("parquet").mode("overwrite").save(self.dst_path)

    def run(self) -> None:
        """
        Main method responsible for executing workflow.
        """
        self.read_file()
        print("[INFO] File was loaded.")
        calc_df = self.calculate_avg()
        print("[INFO] Transformations done.")
        self.save_file(df=calc_df)
        print("[INFO] File was saved.")

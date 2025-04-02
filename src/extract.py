from pyspark.sql import SparkSession

class Reader:
    def __init__(self, spark):
        """
        Initializes the SparkSession with the given sparkSession name.
        """
        self.spark = spark

    def read_csv(self, file_path, header=True, infer_schema=True):
        """
        Reads a file and returns a PySpark DataFrame.

        :param file_path: Path to the CSV file.
        :param header: Indicates if the CSV has headers. Default is True.
        :param infer_schema: Automatically infers the schema. Default is True.
        :return: PySpark DataFrame.
        """
        return self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)
from pyspark.sql import SparkSession
import sqlite3

class ParquetProcessor:
    def __init__(self, spark: SparkSession, parquet_path, sqlite_db, table_name):
        """
        Class constructor.
        :parram spark: Spark session
        :param parquet_path: Path to the Parquet file.
        :param sqlite_db: Path to the SQLite database file.
        :param table_name: Name of the SQLite table where the data will be stored.
        """
        self.spark = spark
        self.parquet_path = parquet_path
        self.sqlite_db = sqlite_db
        self.table_name = table_name
    
    def process(self):
        try:
            self._read_parquet()
            self._save_to_sqlite()
        except Exception as e:
            print(f"Error! in Load data for the {self.table_name} table: {e}")

    
    def _read_parquet(self):
        """
        Read the Parquet file into a PySpark DataFrame.
        """
        self.dataframe = self.spark.read.parquet(self.parquet_path)

    def _save_to_sqlite(self):
        """
        Save the PySpark DataFrame to an SQLite database.
        """
        # Convert PySpark DataFrame to Pandas DataFrame for SQLite insertion
        pandas_df = self.dataframe.toPandas()

        # Save to SQLite
        conn = sqlite3.connect(self.sqlite_db)
        pandas_df.to_sql(self.table_name, conn, if_exists="replace", index=False)
        conn.close()



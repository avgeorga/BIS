from pyspark.sql import DataFrame, SparkSession
from pydantic import BaseModel, Field, ValidationError
from typing import Optional


class TransformProduct:
    def __init__(self, spark: SparkSession, df: DataFrame):
        """
        Initializes the class with the given Spark DataFrame.

        Parameters:
        df (DataFrame): The Spark DataFrame to be used for processing.
        """
        self.product_df = df
        self.spark = spark

    def validate_load(self):
        # Remove record with Description Null
        df_clean = self.product_df.dropna(subset=["Description"])

        # Remove duplicate data
        df_clean = df_clean.dropDuplicates(["StockCode"])

        # Read partiton for validation data
        df_clean.foreachPartition(_validate_partition)

        # Store partiton for next process in parquet format
        df_clean.write.parquet("../data/out/products.parquet", mode="overwrite")  

# Model for data validation
class Products(BaseModel):
    StockCode: str
    Description: str = Field(None, pattern=r'^[A-Za-z\s]*$')
    UnitPrice: float

# Process data fo each parition
def _validate_partition(iterator):
    for row in iterator:
        try:
            _validate_row(row.asDict())
        except Exception as e:
            print(f"Error in Product! {e}")

# Validate data in base to the Pydantic model    
def _validate_row(row):
    try:
        product = Products(StockCode=row["StockCode"], Description=row["Description"], UnitPrice=row["UnitPrice"])
        return product
    except ValidationError as e:
        return f"Error product data validation!: {e}"

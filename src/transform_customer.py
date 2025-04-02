from pyspark.sql import DataFrame, SparkSession
from pydantic import BaseModel, Field, conint, ValidationError
from typing import List


class TransformCustomer:
    def __init__(self, spark: SparkSession, df: DataFrame):
        """
        Initializes the class with the given Spark DataFrame.

        Parameters:
        df (DataFrame): The Spark DataFrame to be used for processing.
        """
        self.customer_df = df
        self.spark = spark

    def validate_load(self):
        # Remove record with CustomerID Null
        df_clean = self.customer_df.dropna(subset=["CustomerID"])

        # Read partiton for validation data
        df_clean.foreachPartition(_validate_partition)

        # Store partiton for next process in parquet format
        df_clean.write.parquet("../data/out/customers.parquet", mode="overwrite")  

# Model for data validation
class Customer(BaseModel):
    CustomerID: int
    Country: str = None

# Process data fo each parition
def _validate_partition(iterator):
    for row in iterator:
        try:
            _validate_row(row.asDict())
        except Exception as e:
            print(f"Error! in Customer: {e}")

# Validate data in base to the Pydantic model    
def _validate_row(row):
    try:
        customer = Customer(CustomerID=row["CustomerID"], Country=row["Country"])
        return customer
    except ValidationError as e:
        return f"Error! customer data validation: {e}"

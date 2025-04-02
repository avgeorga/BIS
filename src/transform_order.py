from pyspark.sql import DataFrame, SparkSession
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime

class TransformOrder:
    def __init__(self, spark: SparkSession, df: DataFrame):
        """
        Initializes the class with the given Spark DataFrame.

        Parameters:
        df (DataFrame): The Spark DataFrame to be used for processing.
        """
        self.order_df = df
        self.spark = spark

    def validate_load(self):
        
        # Read partiton for validation data
        self.order_df.foreachPartition(_validate_partition)

        # Store partiton for next process in parquet format
        self.order_df.write.parquet("../data/out/orders.parquet", mode="overwrite")  

# Model for data validation
class Orders(BaseModel):
    InvoiceNo: int
    StockCode: str
    Quantity: int
    InvoiceDate: datetime
    CustomerID: int

# Process data fo each parition
def _validate_partition(iterator):
    for row in iterator:
        try:
            _validate_row(row.asDict())
        except Exception as e:
            print(f"Error! in Order: {e}")

# Validate data in base to the Pydantic model    
def _validate_row(row):
    try:
        order = Orders(InvoiceNo=row["InvoiceNo"], 
                           StockCode=row["StockCode"], 
                           Quantity=row["Quantity"],
                           CustomerID=row["CustomerID"])
        return order
    except ValidationError as e:
        return f"Error! order data validation: {e}"

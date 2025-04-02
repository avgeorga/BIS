from pyspark.sql import SparkSession
from pydantic import BaseModel, ValidationError
from extract import Reader
from transform_customer import TransformCustomer
from transform_product import TransformProduct
from transform_order import TransformOrder
from load_data import ParquetProcessor


def extract_data(spark, file_name):
    reader = Reader(spark)
    df = reader.read_csv(file_name)
    return df

def extract_validate(spark):
    # Extact Customer data and instance the TransformCustomer class
    transformCustomer = TransformCustomer(spark, extract_data(spark,'../data/in/customers.csv'))
    transformCustomer.validate_load()
    
    # Extact Product data and instance the TransformProduct class
    transformProduct = TransformProduct(spark, extract_data(spark,'../data/in/products.csv'))
    transformProduct.validate_load()

    # Extact Order data and instance the TransformOrder class
    transformOrder = TransformOrder(spark, extract_data(spark,'../data/in/orders.csv'))
    transformOrder.validate_load()

def transform_load(spark):
    # Load customer
    load_customer_data = ParquetProcessor(spark,'../data/out/customers.parquet', '../data/db/bis.db', 'CUSTOMERS')
    load_customer_data.process()

    # Load product
    load_customer_data = ParquetProcessor(spark,'../data/out/products.parquet', '../data/db/bis.db', 'PRODUCTS')
    load_customer_data.process()

    # Load order
    load_customer_data = ParquetProcessor(spark,'../data/out/orders.parquet', '../data/db/bis.db', 'ORDERS')
    load_customer_data.process()
    
    
if __name__ == "__main__":
    """
    Entry point of the script. Creates a SparkSession and calls extract_validate.
    Handles exceptions for general errors and data validation errors.
    """
    try:
        spark=SparkSession.builder.appName("BIS_Example").getOrCreate()
        # Extrac the data from the csv, validate them and store in a parquet
        extract_validate(spark)

        #Read the data from parquet and laod them into SQlite for thier analysis
        transform_load(spark)
        
    except Exception as e:
        print(f"General error: {e}")
    except ValidationError as e:
        print(f"Error validating data: {e}")


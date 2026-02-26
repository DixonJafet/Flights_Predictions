# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import uuid
from datetime import datetime

# COMMAND ----------

def get_airports_info(params):
    """
    Fetch flight data from Aviation Stack API
    """
    url = f"https://api.aviationstack.com/v1/airports"

    offset = 0
    total = 100
    list_results = []

    while (offset < total):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            total = data.get("pagination", {}).get("total", 0)
            list_results.extend(data.get("data", []))
            offset += params["limit"]
            params["offset"] = offset 
        except Exception as e:
            print(f"Error fetching flights: {e}")
            return []
        
    return list_results


# COMMAND ----------

def transform_flight_data(airports):
    transformed_data = []
    for airport in airports:
        record = {
            "id": airport.get("icao_code"),
            "airport_name": airport.get("airport_name"),
            "country_name": airport.get("country_name")
        }
        transformed_data.append(record)
    return transformed_data

# COMMAND ----------

def load_airports_to_delta_lake(data, database_name="aviation_db", table_name="airports"):
    """
    Load transformed data to Delta Lake
    """
    if not data:
        print("No data to load")
        return
    
    # Define explicit schema to ensure all columns are present
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("airport_name", StringType(), True),
        StructField("country_name", StringType(), True),
    ])
    
    # Convert to Pandas DataFrame first
    pdf = pd.DataFrame(data)
    
    # Convert to Spark DataFrame with explicit schema
    df = spark.createDataFrame(pdf, schema=schema)
    
    # Ensure database exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
    
    # Write to Delta table
    full_table_name = f"{database_name}.{table_name}"
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)
    
    print(f"Successfully loaded {len(data)} records to {full_table_name}")
    
    return df

# COMMAND ----------

def pipeline_fetch_airports(params, database_name, table_name):
    """
    Fetch airport data using the Aviation Stack API
    """
    airports_data = get_airports_info(params)
    transformed_data = transform_flight_data(airports_data)
    df = load_airports_to_delta_lake(transformed_data, database_name, table_name)
    
    return df

# COMMAND ----------

if __name__ == "__main__":

    api_key = dbutils.secrets.get(
        scope="my-scope",
        key="extra-api-key"
    )
    

    # Replace with your actual API key
    API_KEY = api_key
    
    # Run the ETL
    result_df = pipeline_fetch_airports(   
        {
            "access_key": api_key,
            "limit": 900,
            "offset": 0
        } , 
        database_name="aviation_db",
        table_name="airports",# Adjust based on your API plan limits
    )
    
    # Display sample results
    display(result_df.limit(10))
    


# COMMAND ----------

    # Query the Delta table
spark.sql("SELECT * FROM aviation_db.airports LIMIT 10").show()
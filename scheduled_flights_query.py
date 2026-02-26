# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import uuid
from datetime import datetime

# COMMAND ----------

def get_country_from_airport(icao_code):
    """
    Fetch country information from airport ICAO code
    """
    if not icao_code:
        return None
    
    try:
        
        country = spark.sql(f"SELECT country_name FROM aviation_db.airports WHERE id = '{icao_code}'").collect()[0][0]
        return country
    except Exception as e:
        print(f"Error gething country for ICAO {icao_code}: {e}")
        return None

# COMMAND ----------

def fetch_scheduled_flights(params):
    """
    Fetch flight data from Aviation Stack API
    """
    url = "https://api.aviationstack.com/v1/flights"

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

def transform_flight_data(flights):
    """
    Transform flight data to match target schema
    """
    transformed_data = []
    
    for flight in flights:
        try:
            # Extract departure info
            departure = flight.get("departure", {})
            dep_icao = departure.get("icao")
            
            # Extract arrival info
            arrival = flight.get("arrival", {})
            arr_icao = arrival.get("icao")
            
            # Get countries with caching
            if (dep_icao) and (arr_icao):
                departure_country = get_country_from_airport(dep_icao)
                arrival_country = get_country_from_airport(arr_icao)
                if departure_country and arrival_country: 
                    
                    # Create transformed record
                    record = {
                        "id": str(uuid.uuid4()),
                        "flight_date": flight.get("flight_date"),
                        "airline_name": flight.get("airline", {}).get("name"),
                        "departure_airport": departure.get("airport"),
                        "departure_country": departure_country,
                        "departure_timezone": departure.get("timezone"),
                        "departure_scheduled_time": departure.get("scheduled"),
                        "arrival_airport": arrival.get("airport"),
                        "arrival_country": arrival_country,
                        "arrival_timezone": arrival.get("timezone"),
                        "arrival_scheduled_time": arrival.get("scheduled")
                    }
                    
                    transformed_data.append(record)
            
        except Exception as e:
            print(f"Error transforming flight record: {e}")
            continue
    
    return transformed_data

# COMMAND ----------

def load_to_delta_lake(data, database_name="aviation_db", table_name="scheduled_flights"):
    """
    Load transformed data to Delta Lake
    """
    if not data:
        print("No data to load")
        return
    
    # Define explicit schema to ensure all columns are present
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("flight_date", StringType(), True),
        StructField("airline_name", StringType(), True),
        StructField("departure_airport", StringType(), True),
        StructField("departure_country", StringType(), True),
        StructField("departure_timezone", StringType(), True),
        StructField("departure_scheduled_time", StringType(), True),
        StructField("arrival_airport", StringType(), True),
        StructField("arrival_country", StringType(), True),
        StructField("arrival_timezone", StringType(), True),
        StructField("arrival_scheduled_time", StringType(), True)
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

def run_etl_pipeline(api_key, database_name="aviation_db", table_name="scheduled_flights", limit=100):
    """
    Run the complete ETL pipeline
    """
    print("Starting ETL Pipeline...")


 
    # Extract
    print("Extracting flight data from API...")
    arr_scheduled_flights = fetch_scheduled_flights(   params = {
        "access_key": api_key,
        "limit": limit,
        "arr_icao": "MROC",  # Example ICAO codes for Costa Rica airports
        "flight_status": "scheduled"
    })

    dep_scheduled_flights = fetch_scheduled_flights(   params = {
        "access_key": api_key,
        "limit": limit,
        "dep_icao": "MROC",  # Example ICAO codes for Costa Rica airports
        "flight_status": "scheduled"
    })






    flights = arr_scheduled_flights + dep_scheduled_flights


    print(f"Extracted {len(flights)} flights")
    
    # Transform
    print("Transforming flight data...")
    transformed_data = transform_flight_data(flights)
    print(f"Transformed {len(transformed_data)} records")
    
    # Load
    print("Loading data to Delta Lake...")
    df = load_to_delta_lake(transformed_data, database_name, table_name)
    
    print("ETL Pipeline completed successfully!")
    
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
    result_df = run_etl_pipeline(
        api_key=API_KEY,
        database_name="aviation_db",
        table_name="scheduled_flights",
        limit=100  # Adjust based on your API plan limits
    )
    
    # Display sample results
    display(result_df.limit(10))
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp, hour, dayofweek, month, datediff, lit, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

# COMMAND ----------

# ============================================
# STEP 1: PREPARE TRAINING DATA FROM FLIGHTS
# ============================================

def prepare_training_data(database_name="aviation_db", flights_table="flights"):
    """
    Load and prepare training data from historical flights
    """
    print("Loading training data from flights table...")
    
    # Read the flights table
    df = spark.table(f"{database_name}.{flights_table}")
    
    # Filter out records with missing actual times (can't train without labels)
    df = df.filter(
        col("departure_actual_time").isNotNull() & 
        col("arrival_actual_time").isNotNull() &
        col("departure_scheduled_time").isNotNull() &
        col("arrival_scheduled_time").isNotNull()
    )
    
    # Convert timestamp strings to timestamp type
    df = df.withColumn("departure_scheduled_ts", to_timestamp(col("departure_scheduled_time")))
    df = df.withColumn("departure_actual_ts", to_timestamp(col("departure_actual_time")))
    df = df.withColumn("arrival_scheduled_ts", to_timestamp(col("arrival_scheduled_time")))
    df = df.withColumn("arrival_actual_ts", to_timestamp(col("arrival_actual_time")))
    
    # Calculate target variables (delay in minutes)
    df = df.withColumn(
        "departure_delay_minutes",
        (unix_timestamp("departure_actual_ts") - unix_timestamp("departure_scheduled_ts")) / 60
    )
    
    df = df.withColumn(
        "arrival_delay_minutes",
        (unix_timestamp("arrival_actual_ts") - unix_timestamp("arrival_scheduled_ts")) / 60
    )
    
    # Extract time-based features
    df = df.withColumn("scheduled_hour", hour(col("departure_scheduled_ts")))
    df = df.withColumn("scheduled_day_of_week", dayofweek(col("departure_scheduled_ts")))
    df = df.withColumn("scheduled_month", month(col("departure_scheduled_ts")))
    
    # Fill nulls for delay_time
    df = df.withColumn("delay_time", when(col("delay_time").isNull(), 0).otherwise(col("delay_time")))
    
    print(f"Training data prepared: {df.count()} records")
    
    return df


# COMMAND ----------

# DBTITLE 1,Cell 3
# ============================================
# STEP 2: BUILD PREDICTION MODELS
# ============================================

def build_prediction_models(training_data):
    """
    Build separate models for departure and arrival predictions
    """
    print("Building prediction models...")
    
    # Define categorical columns
    categorical_cols = ["airline_name", "departure_airport", "arrival_airport", 
                       "departure_country", "arrival_country"]
    
    # Define numeric columns
    numeric_cols = ["delay_time", "scheduled_hour", "scheduled_day_of_week", "scheduled_month"]
    
    # String Indexers for categorical variables
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        for col in categorical_cols
    ]
    
    # One-Hot Encoders
    encoders = [
        OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded")
        for col in categorical_cols
    ]
    
    # Feature columns after encoding
    encoded_cols = [f"{col}_encoded" for col in categorical_cols]
    feature_cols = encoded_cols + numeric_cols
    
    # Vector Assembler
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    
    # ---- Model for Departure Delay ----
    departure_model = GBTRegressor(
        featuresCol="features",
        labelCol="departure_delay_minutes",
        predictionCol="predicted_departure_delay",
        maxIter=50,
        maxDepth=5
    )
    
    # Pipeline for departure prediction
    departure_pipeline = Pipeline(stages=indexers + encoders + [assembler, departure_model])
    
    print("Training departure delay model...")
    departure_pipeline_model = departure_pipeline.fit(training_data)
    
    # ---- Model for Arrival Delay ----
    arrival_model = GBTRegressor(
        featuresCol="features",
        labelCol="arrival_delay_minutes",
        predictionCol="predicted_arrival_delay",
        maxIter=50,
        maxDepth=5
    )
    
    # Pipeline for arrival prediction
    arrival_pipeline = Pipeline(stages=indexers + encoders + [assembler, arrival_model])
    
    print("Training arrival delay model...")
    arrival_pipeline_model = arrival_pipeline.fit(training_data)
    
    return departure_pipeline_model, arrival_pipeline_model

# COMMAND ----------

# ============================================
# STEP 3: EVALUATE MODELS
# ============================================

def evaluate_models(training_data, departure_model, arrival_model):
    """
    Evaluate model performance
    """
    print("\nEvaluating models...")
    
    # Split data for evaluation
    train, test = training_data.randomSplit([0.8, 0.2], seed=42)
    
    # Predict on test set
    departure_predictions = departure_model.transform(test)
    arrival_predictions = arrival_model.transform(test)
    
    # Evaluators
    evaluator_rmse = RegressionEvaluator(metricName="rmse")
    evaluator_mae = RegressionEvaluator(metricName="mae")
    
    # Departure model metrics
    dep_rmse = evaluator_rmse.evaluate(
        departure_predictions, 
        {evaluator_rmse.labelCol: "departure_delay_minutes", 
         evaluator_rmse.predictionCol: "predicted_departure_delay"}
    )
    dep_mae = evaluator_mae.evaluate(
        departure_predictions,
        {evaluator_mae.labelCol: "departure_delay_minutes",
         evaluator_mae.predictionCol: "predicted_departure_delay"}
    )
    
    # Arrival model metrics
    arr_rmse = evaluator_rmse.evaluate(
        arrival_predictions,
        {evaluator_rmse.labelCol: "arrival_delay_minutes",
         evaluator_rmse.predictionCol: "predicted_arrival_delay"}
    )
    arr_mae = evaluator_mae.evaluate(
        arrival_predictions,
        {evaluator_mae.labelCol: "arrival_delay_minutes",
         evaluator_mae.predictionCol: "predicted_arrival_delay"}
    )
    
    print(f"\nDeparture Model - RMSE: {dep_rmse:.2f} minutes, MAE: {dep_mae:.2f} minutes")
    print(f"Arrival Model - RMSE: {arr_rmse:.2f} minutes, MAE: {arr_mae:.2f} minutes")
    
    return departure_predictions, arrival_predictions

# COMMAND ----------

# ============================================
# STEP 4: PREDICT ON SCHEDULED FLIGHTS
# ============================================

def predict_scheduled_flights(departure_model, arrival_model, 
                              database_name="aviation_db", 
                              scheduled_table="scheduled_flights"):
    """
    Apply predictions to scheduled_flights table
    """
    print(f"\nLoading scheduled flights from {database_name}.{scheduled_table}...")
    
    # 1. Read scheduled flights
    scheduled_df = spark.table(f"{database_name}.{scheduled_table}")
    
    # 2. Preprocess: Convert scheduled times to timestamp
    scheduled_df = scheduled_df.withColumn(
        "departure_scheduled_ts", 
        to_timestamp(col("departure_scheduled_time"))
    ).withColumn(
        "arrival_scheduled_ts",
        to_timestamp(col("arrival_scheduled_time"))
    )
    
    # 3. Feature Extraction: hour, day, month
    scheduled_df = scheduled_df.withColumn("scheduled_hour", hour(col("departure_scheduled_ts")))
    scheduled_df = scheduled_df.withColumn("scheduled_day_of_week", dayofweek(col("departure_scheduled_ts")))
    scheduled_df = scheduled_df.withColumn("scheduled_month", month(col("departure_scheduled_ts")))
    
    # 4. Handle delay_time nulls
    if "delay_time" in scheduled_df.columns:
        scheduled_df = scheduled_df.withColumn("delay_time", when(col("delay_time").isNull(), 0).otherwise(col("delay_time")))
    else:
        scheduled_df = scheduled_df.withColumn("delay_time", lit(0))
    
    # 5. FIX: Make departure predictions first
    print("Making departure delay predictions...")
    dep_results = departure_model.transform(scheduled_df)
    
    # We rename the prediction and drop the shared pipeline columns to avoid conflicts
    dep_results = dep_results.withColumnRenamed("predicted_departure_delay", "final_dep_delay")
    
    # 6. FIX: Make arrival predictions using the original scheduled_df
    # This prevents the "column already exists" error
    print("Making arrival delay predictions...")
    arr_results = arrival_model.transform(scheduled_df)
    
    # 7. Combine results back together on 'id'
    result_df = arr_results.join(
        dep_results.select("id", "final_dep_delay"), 
        on="id"
    )
    
    # 8. Calculate final estimated times
    # Prediction is in minutes, so we multiply by 60 for unix_timestamp addition
    result_df = result_df.withColumn(
        "departure_estimated_time",
        to_timestamp(unix_timestamp("departure_scheduled_ts") + (col("final_dep_delay") * 60))
    ).withColumn(
        "arrival_estimated_time",
        to_timestamp(unix_timestamp("arrival_scheduled_ts") + (col("predicted_arrival_delay") * 60))
    )
    
    # 9. Select final relevant columns
    final_output = result_df.select(
        "id",
        "flight_date",
        "airline_name",
        "departure_airport",
        "arrival_airport",
        "departure_scheduled_time",
        "arrival_scheduled_time",
        "departure_estimated_time",
        "arrival_estimated_time",
        col("final_dep_delay").alias("predicted_departure_delay"),
        "predicted_arrival_delay"
    )
    
    print(f"Predictions completed for {final_output.count()} scheduled flights")
    
    return final_output

# COMMAND ----------

# ============================================
# STEP 5: UPDATE SCHEDULED_FLIGHTS TABLE
# ============================================

def update_scheduled_flights_table(predictions_df, database_name="aviation_db", 
                                   scheduled_table="scheduled_flights"):
    """
    Update the scheduled_flights table with predictions
    """
    print(f"\nUpdating {database_name}.{scheduled_table} with predictions...")
    
    # Create temporary view
    predictions_df.createOrReplaceTempView("predictions_temp")
    
    # Merge predictions back into scheduled_flights table
    merge_query = f"""
    MERGE INTO {database_name}.{scheduled_table} AS target
    USING predictions_temp AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        target.departure_estimated_time = source.departure_estimated_time,
        target.arrival_estimated_time = source.arrival_estimated_time
    """
    
    spark.sql(merge_query)
    
    print("Update completed successfully!")


# COMMAND ----------

# ============================================
# MAIN PIPELINE
# ============================================

def run_prediction_pipeline(database_name="aviation_db", 
                           flights_table="flights",
                           scheduled_table="scheduled_flights"):
    """
    Complete ML pipeline: Train, Evaluate, Predict, Update
    """
    print("=" * 60)
    print("STARTING FLIGHT PREDICTION PIPELINE")
    print("=" * 60)
    
    # Step 1: Prepare training data
    training_data = prepare_training_data(database_name, flights_table)
    
    # Step 2: Build models
    departure_model, arrival_model = build_prediction_models(training_data)
    
    # Step 3: Evaluate models
    dep_pred, arr_pred = evaluate_models(training_data, departure_model, arrival_model)
    
    # Step 4: Predict on scheduled flights
    predictions_df = predict_scheduled_flights(departure_model, arrival_model, 
                                               database_name, scheduled_table)
    
    # Step 5: Update table
    update_scheduled_flights_table(predictions_df, database_name, scheduled_table)
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    return predictions_df

# COMMAND ----------

# ============================================
# EXECUTE PIPELINE
# ============================================

if __name__ == "__main__":
    # Run the complete pipeline
    predictions = run_prediction_pipeline(
        database_name="aviation_db",
        flights_table="flights",
        scheduled_table="scheduled_flights"
    )
    
    # Display sample predictions
    print("\nSample Predictions:")
    display(predictions.limit(20))
    
    # Verify updates in the table
    print("\nVerifying updated scheduled_flights table:")
    updated_table = spark.sql("""
        SELECT 
            id,
            airline_name,
            departure_airport,
            arrival_airport,
            departure_scheduled_time,
            departure_estimated_time,
            arrival_scheduled_time,
            arrival_estimated_time
        FROM aviation_db.scheduled_flights
        LIMIT 10
    """)
    display(updated_table)

🛫 Costa Rica Flight Prediction Pipeline
An end-to-end Data Engineering and Machine Learning pipeline built entirely on Databricks. This project orchestrates the collection of historical flight data, real-time scheduled flights in Costa Rica, and uses Machine Learning to predict arrival and departure delays.

🔗 Live Project Dashboard
https://dixonjafet.github.io/Flight_Dashboard_DataBricks/



🛠️ Tech Stack
-Platform: Databricks (Cloud Version)

-Orchestration: Databricks Workflows (Jobs & Pipelines)

-Languages: Python (PySpark/Pandas), SQL

-Storage: Delta Lake

-Compute: Serverless Starter Warehouse / Standard Clusters

-Visualization: GitHub Pages (Custom Dashboard)


⚙️ Pipeline Architecture
The pipeline (Flight_Pipeline) consists of five main stages:

Get_Airports_Codes: Fetches and updates the reference data for international and local airport codes relevant to the Costa Rican flight paths.

Get_ended_flights: Ingests historical data of completed flights. This serves as the "Ground Truth" for training our predictive models.

reset_today_flightsDB: A maintenance task that cleans and resets the daily operational tables to ensure the dashboard reflects current-day data without duplicates.

Today_Scheduled_flights_query: Pulls real-time API/Source data for flights scheduled for the current date.

train_estimated_time_model: The ML component. It processes historical patterns to train/apply a model that predicts the estimated_time for the currently scheduled flights.

📊 Dashboard & Predictions
The output of this pipeline is consumed by a front-end dashboard where users can compare:

Scheduled Time: The official time provided by airlines.

Predicted Time: The ML-calculated time based on historical delay trends and current flight status.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import threading
import time

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0 pyspark-shell'


# Créer une session Spark
spark = SparkSession.builder \
    .appName("IoT Alerts Dashboard") \
    .getOrCreate()

# Schéma qui définit les données du capteur
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("rainfall", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("UV_index", DoubleType(), True)
])

# Kafka setup
bootstrapServers = "pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092"
topics = "sensor"

# lire flow de data Kafka 
raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrapServers) \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"LIQN5IFO4VF2U5KV\" password=\"Qp6Zl3PuSayJSCRD8/pXJBJNNPIE6NCKWIMUsUk701c2HX6Pf/zVFg2Z1kVJzJei\";") \
            .option("subscribe", topics) \
            .option("startingOffsets", "earliest") \
            .load()


# Analyser les données Kafka
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Définir des règles d'alerte
alerts = parsed_stream.filter(
    (col("temperature") > 35.0) | 
    (col("humidity") < 35.0) | 
    (col("UV_index") > 8.0) | 
    (col("rainfall") > 40.0) | 
    (col("wind_speed") > 7.0)
).withColumn(
    "alert",
    when(col("temperature") > 35.0, "High Temperature")
    .when(col("humidity") < 35.0, "Low Humidity")
    .when(col("UV_index") > 8.0, "High UV Index")
    .when(col("rainfall") > 40.0, "Heavy Rainfall")
    .when(col("wind_speed") > 7.0, "High Wind Speed")
    .otherwise("No Alert")
)

# Définir le chemin de sortie
ALERTS_PATH = "./alerts_parquet"
CHECKPOINT_PATH = "./alerts_checkpoint"

# Écrire les données d'alerte dans Parquet
alerts_query = alerts.writeStream \
    .format("parquet") \
    .option("path", ALERTS_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()
    

# Initialisez l'application Dash
app = Dash(__name__)

alerts_data = pd.DataFrame()

# Charger périodiquement les données des fichiers Parquet
def fetch_alert_data():
    global alerts_data
    try:
        alerts_data = spark.read.parquet(ALERTS_PATH).toPandas()
        #alerts_data['timestamp'] = pd.to_datetime(alerts_data['timestamp'].astype(float), unit='s')
    except Exception as e:
        print(f"Erreur de lecture du fichier Parquet: {e}")
        alerts_data = pd.DataFrame()

# Définir la disposition de l'application Dash
app.layout = html.Div([
    html.H1("IoT Alerts Dashboard", style={'textAlign': 'center'}),

    dcc.Interval(id='update-interval', interval=5000),  

    # Créer un graphique indépendant pour chaque variable
    html.Div([
        html.H2("Sensor Metrics Over Time"),
        dcc.Graph(id='temperature-trend'),
        dcc.Graph(id='humidity-trend'),
        dcc.Graph(id='UV-index-trend'),
        dcc.Graph(id='rainfall-trend'),
        dcc.Graph(id='wind-speed-trend')
    ]),

    # Carte de répartition des alertes
    html.H2("Number of Alerts"),
    dcc.Graph(id='alert-distribution')
])


# Mettre à jour le graphique de tendance de la température
@app.callback(
    Output('temperature-trend', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_temperature_trend(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.scatter(title="No Data Available")
    return px.line(alerts_data, x="timestamp", y="temperature",
                   title="Temperature Over Time",
                   labels={"temperature": "Temperature (°C)", "timestamp": "Timestamp"},
                   color_discrete_sequence=["orange"])


# Mettre à jour le graphique de tendance de la humidité
@app.callback(
    Output('humidity-trend', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_humidity_trend(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.scatter(title="No Data Available")
        #alerts_data['timestamp'] = pd.to_datetime(alerts_data['timestamp'], unit='s') 
    return px.line(alerts_data, x="timestamp", y="humidity",
                   title="Humidity Over Time",
                   labels={"humidity": "Humidity (%)", "timestamp": "Timestamp"},
                   color_discrete_sequence=["purple"])


# Mettre à jour le graphique de tendance de la indice uv
@app.callback(
    Output('UV-index-trend', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_uv_index_trend(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.scatter(title="No Data Available")
    return px.line(alerts_data, x="timestamp", y="UV_index",
                   title="UV Index Over Time",
                   labels={"UV_index": "UV Index", "timestamp": "Timestamp"},
                   color_discrete_sequence=["red"]) 


# Mettre à jour le graphique de tendance de la pluie
@app.callback(
    Output('rainfall-trend', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_rainfall_trend(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.scatter(title="No Data Available")
    return px.line(alerts_data, x="timestamp", y="rainfall",
                   title="Rainfall Over Time",
                   labels={"rainfall": "Rainfall (mm)", "timestamp": "Timestamp"},
                   color_discrete_sequence=["blue"])


# Mettre à jour le graphique de tendance de la vitesse du vent 
@app.callback(
    Output('wind-speed-trend', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_wind_speed_trend(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.scatter(title="No Data Available")
    return px.line(alerts_data, x="timestamp", y="wind_speed",
                   title="Wind Speed Over Time",
                   labels={"wind_speed": "Wind Speed (km/h)", "timestamp": "Timestamp"},
                   color_discrete_sequence=["green"])


# Mettre à jour la carte de distribution des alertes
@app.callback(
    Output('alert-distribution', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_alert_distribution(n_intervals):
    global alerts_data
    fetch_alert_data()
    if alerts_data.empty:
        return px.bar(title="No Data Available")
    alert_counts = alerts_data['alert'].value_counts().reset_index()
    alert_counts.columns = ['Alert Type', 'Count']
    return px.bar(alert_counts, x='Alert Type', y='Count', color='Alert Type',
                  title="Alert Type Distribution",
                  labels={'Count': 'Number of Alerts', 'Alert Type': 'Type of Alert'})


# Entrée du programme principal
if __name__ == "__main__":
    # Le fil principal exécute l'application Dash
    app.run_server(debug=True, port=8050)

    # Le thread enfant exécute le streaming
    def start_streaming():
        alerts_query.awaitTermination()
    
    threading.Thread(target=start_streaming, daemon=True).start()
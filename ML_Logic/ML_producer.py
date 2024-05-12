from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, array
from kafka import KafkaProducer
import json

# Define a UDF to flatten and calculate mean
def flatten_and_mean(features):
    if features and isinstance(features[0], list):
        flattened = [item for sublist in features for item in sublist]
        if flattened and isinstance(flattened[0], list):
            flattened = [item for sublist in flattened for item in sublist]
        return float(sum(flattened) / len(flattened)) if flattened else 0.0
    elif features and isinstance(features[0], (int, float)):
        return float(sum(features) / len(features))
    return 0.0

get_mean_udf = udf(flatten_and_mean, DoubleType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KNN Music Recommendations") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Load data from MongoDB
df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/music_features6.features6").load()

# Create "features" column as a list of spectral centroids, mfcc, and zero crossing rate
df = df.withColumn("features", array("spectral_centroid", "mfcc", "zero_crossing_rate"))

# Drop rows with missing values in the "features" column
df = df.dropna(subset=["features"])

# Apply the UDF to create a new column for the mean of features
df = df.withColumn("mean_features", get_mean_udf(col("features")))

# Define a UDF to convert the mean_features value to a vector
to_vector_udf = udf(lambda x: Vectors.dense([x]), VectorUDT())

# Apply the UDF to create the feature vector column
df = df.withColumn("features", to_vector_udf("mean_features"))

# Drop rows with missing values in the "features" column
df = df.dropna(subset=["features"])

# Scale the features
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), max_block_ms=120000)


# Iterate over DataFrame rows and publish messages to Kafka
while True:
    for idx, row in enumerate(df.collect()):
        message = {"features": row["features"].toArray().tolist(), "file": row["file"]}
        producer.send('song_features_topic6', value=message)
        print(f"Sent message {idx + 1} to Kafka")

    print("All messages sent to Kafka")

    # Stop SparkSession
    spark.stop()
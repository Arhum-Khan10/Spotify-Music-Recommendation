from flask import Flask, Response, render_template, redirect, url_for
from kafka import KafkaConsumer
import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from sklearn.neighbors import NearestNeighbors
from pyspark.sql.functions import udf, col, array
from pyspark.sql import Row
import random
import pickle

app = Flask(__name__)

base_audio_dir = '/home/aaqib/Downloads/BDA-Project/BDA-Project/fma_small1'
metadata_file = 'fma_metadata/raw_tracks.csv'

# Load metadata from CSV file
metadata_df = pd.read_csv(metadata_file)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'audio_files_topic11',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Function to stream audio file
def stream_audio(file_path):
    with open(file_path, 'rb') as audio_file:
        audio_data = audio_file.read()
    return Response(audio_data, mimetype='audio/mpeg')

@app.route('/')
def list_audio_files():
    audio_files = []
    for root, dirs, files in os.walk(base_audio_dir):
        for file in files:
            if file.endswith(('.mp3', '.wav')):
                # Extract track ID from file name
                track_id = os.path.splitext(file)[0]
                # Format track ID with leading zeros to six digits
                track_id_padded = track_id.zfill(6)
                # Lookup metadata using track ID
                metadata_row = metadata_df.loc[metadata_df['track_id'] == int(track_id)]
                if not metadata_row.empty:
                    album_title = metadata_row['album_title'].values[0]
                    artist_name = metadata_row['artist_name'].values[0]
                    track_title = metadata_row['track_title'].values[0]
                    relative_path = os.path.relpath(os.path.join(root, file), base_audio_dir)
                    audio_files.append({'file_name': file, 'album_title': album_title, 'artist_name': artist_name, 'track_title': track_title, 'relative_path': relative_path})

    return render_template('index.html', audio_files=audio_files)

@app.route('/play/<path:audio_path>')
def play_audio(audio_path):
    file_path = os.path.join(base_audio_dir, audio_path)
    return stream_audio(file_path)

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

# Route for recommendation page
@app.route('/recommendation')
def recommendation():

    # Initialize SparkSession
    spark = SparkSession.builder \
    .appName("KNN Music Recommendations") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


    # Load data from MongoDB
    df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/song_features.features").load()

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

    # Displaying the query song
    query_song = random.choice(df.select("features").collect())[0]

    # Create a Row object with a "features" column
    query_song_row = Row(features=query_song)

    # Create a DataFrame with the Row object
    query_song_df = spark.createDataFrame([query_song_row])

    # Transform the DataFrame
    transformed_query_song_df = scaler_model.transform(query_song_df)

    # Get the features of the query song as a Vector
    query_song_vector = query_song_df.first().features

    # Collect features as numpy array
    features_array = df.select("scaled_features").rdd.map(lambda row: row.scaled_features.toArray()).collect()

    # Initialize KNN model
    knn = NearestNeighbors(n_neighbors=5, algorithm='auto')

    # Fit KNN model
    knn.fit(features_array)

    # Displaying the query song
    query_song = random.choice(df.select("scaled_features").collect())[0].toArray()

    # Find nearest neighbors for query song
    distances, indices = knn.kneighbors([query_song])

    # Get the file names of nearest neighbors
    nearest_neighbor_files = [df.select("file").collect()[i][0] for i in indices[0]]

    # Print the file names of the nearest neighbors
    for neighbor_file in nearest_neighbor_files:
        print(neighbor_file)
        return render_template('recommendation.html', nearest_neighbor_files=nearest_neighbor_files)

if __name__ == '__main__':
    app.run(debug=True, port=5003)

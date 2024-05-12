from flask import Flask, Response, render_template, redirect, url_for
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from sklearn.neighbors import NearestNeighbors
from pyspark.sql.functions import udf, col, array
from pyspark.sql import Row
import numpy as np

app = Flask(__name__)

base_audio_dir = '/home/aaqib/Downloads/BDA-Project/BDA-Project/fma_small1'
metadata_file = 'fma_metadata/raw_tracks.csv'

# Load metadata from CSV file
metadata_df = pd.read_csv(metadata_file)

# Function to stream audio file
def stream_audio(file_path):
    def generate():
        with open(file_path, 'rb') as audio_file:
            while True:
                audio_chunk = audio_file.read(1024)
                if not audio_chunk:
                    break
                yield audio_chunk

    return Response(generate(), mimetype='audio/mpeg')

@app.route('/')
def list_audio_files():
    audio_files = []
    for root, dirs, files in os.walk(base_audio_dir):
        for file in files:
            if file.endswith(('.mp3', '.wav')):
                track_id = os.path.splitext(file)[0]
                track_id_padded = track_id.zfill(6)
                metadata_row = metadata_df[metadata_df['track_id'] == int(track_id)]
                if not metadata_row.empty:
                    album_title = metadata_row['album_title'].values[0]
                    artist_name = metadata_row['artist_name'].values[0]
                    track_title = metadata_row['track_title'].values[0]
                    relative_path = os.path.relpath(os.path.join(root, file), base_audio_dir)
                    audio_files.append({
                        'file_name': file, 
                        'album_title': album_title, 
                        'artist_name': artist_name, 
                        'track_title': track_title, 
                        'relative_path': relative_path
                    })

    return render_template('index.html', audio_files=audio_files)

@app.route('/play/<path:audio_path>')
def play_audio(audio_path):
    file_path = os.path.join(base_audio_dir, audio_path)
    return stream_audio(file_path)

@app.route('/laoding')
def loading():
    return render_template('loading.html')

# Define a UDF to flatten and calculate mean
def flatten_and_mean(features):
    from itertools import chain

    def flatten(lst):
        for item in lst:
            if isinstance(item, list):
                yield from flatten(item)
            else:
                yield item

    # Convert the potentially deeply nested list into a flat list of numbers
    flattened = list(flatten(features))

    # Check if the flattened list is empty
    if not flattened:
        return 0.0

    # Compute the mean of the flattened list
    return float(sum(flattened)) / len(flattened)

get_mean_udf = udf(flatten_and_mean, DoubleType())

@app.route('/recommendation', defaults={'audio_path': None})
@app.route('/recommendation/<audio_path>')
def recommendation(audio_path):
    print(f"recommendation function called with audio_path: {audio_path}")
    song_name = os.path.splitext(os.path.basename(audio_path))[0] + '.mp3'

    spark = SparkSession.builder \
        .appName("KNN Music Recommendations") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/song_features.features").load()
    df = df.withColumn("features", array("spectral_centroid", "mfcc", "zero_crossing_rate"))
    df = df.dropna(subset=["features"])
    df = df.withColumn("mean_features", get_mean_udf(col("features")))

    to_vector_udf = udf(lambda x: Vectors.dense([x]), VectorUDT())
    df = df.withColumn("features", to_vector_udf("mean_features"))
    df = df.dropna(subset=["features"])

    scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    filtered_df = df.filter(df.file == song_name).select("scaled_features").collect()
    if filtered_df:
        query_song = filtered_df[0][0]
    else:
        print(f"No song found with name {song_name}")
        query_song = df.select("scaled_features").collect()[0][0]

    query_song_row = Row(features=query_song)
    query_song_df = spark.createDataFrame([query_song_row])
    transformed_query_song_df = scaler_model.transform(query_song_df)
    query_song_vector = query_song_df.first().features

    features_array = np.array(df.select("scaled_features").rdd.map(lambda row: row.scaled_features.toArray()).collect())
    knn = NearestNeighbors(n_neighbors=5, algorithm='auto')
    knn.fit(features_array)

    distances, indices = knn.kneighbors([query_song_vector.toArray()])
    nearest_neighbor_files = [df.select("file").collect()[i][0] for i in indices[0]]

    return render_template('recommendation.html', song_name=song_name, nearest_neighbor_files=nearest_neighbor_files)

if __name__ == '__main__':
    app.run(debug=True, port=5004)
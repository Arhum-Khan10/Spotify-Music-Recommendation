from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, lit, least, array
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql import Row
import random

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
spark = SparkSession.builder.getOrCreate()

# Load data from MongoDB
df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/music_features2.features2").load()

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

# Define the LSH model
lsh = BucketedRandomProjectionLSH(inputCol="scaled_features", outputCol="hashes", bucketLength=0.1, numHashTables=5)

# Fit the LSH model
model = lsh.fit(df)

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

# Find approximate nearest neighbors using LSH
nearest_neighbors = model.approxNearestNeighbors(df, query_song_vector, numNearestNeighbors=5)

# print the file name of the nearest neighbors
for row in nearest_neighbors.collect():
    print(row.file)

# saving the model file 
model.write().overwrite().save("LSH_model")

# Stop SparkSession
spark.stop()
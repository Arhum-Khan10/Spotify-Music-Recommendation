import random
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf, col, lit, least, array
from pyspark.ml.feature import BucketedRandomProjectionLSH

# Function to flatten the features and calculate the mean
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

spark = SparkSession.builder.getOrCreate()

# Loading data from MongoDB
df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/music_features2.features2").load()

# Creating a new column "features" with the selected features
df = df.withColumn("features", array("spectral_centroid", "mfcc", "zero_crossing_rate"))

df = df.dropna(subset=["features"])

# Applying the UDF to calculate the mean of the features
df = df.withColumn("mean_features", get_mean_udf(col("features")))

# Convert the "mean_features" column to a Dense Vector
to_vector_udf = udf(lambda x: Vectors.dense([x]), VectorUDT())

# Create a new column "features" with the Dense Vector
df = df.withColumn("features", to_vector_udf("mean_features"))

df = df.dropna(subset=["features"])

# Normalize the features
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)

# Creating a LSH model
lsh = BucketedRandomProjectionLSH(inputCol="scaled_features", outputCol="hashes", bucketLength=0.1, numHashTables=5)

model = lsh.fit(df)

query_song = random.choice(df.select("features").collect())[0]

query_song_row = Row(features=query_song)

query_song_df = spark.createDataFrame([query_song_row])

transformed_query_song_df = scaler_model.transform(query_song_df)

# Getting the features of the query song as a Vector
query_song_vector = query_song_df.first().features

# Finding approximate nearest neighbors using LSH
nearest_neighbors = model.approxNearestNeighbors(df, query_song_vector, numNearestNeighbors=5)

for row in nearest_neighbors.collect():
    print(row.file)

# saving the model file 
model.write().overwrite().save("LSH_model")

spark.stop()
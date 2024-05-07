from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, lit, least, array
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
import numpy as np



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


# Define a UDF to calculate Euclidean distance
def calculate_distance(features, center_index):
    center = centers_list[center_index]
    return float(np.linalg.norm(np.array(features) - np.array(center)))

calculate_distance_udf = udf(calculate_distance, DoubleType())

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

# Define the Gaussian Mixture Model (GMM) with 5 clusters
gmm = GaussianMixture(k=5, seed=1)

# Fit the model
model = gmm.fit(df)

# Get the cluster centers
centers = model.gaussiansDF.select("mean").collect()

# query the 3rd song
query_song = df.select("features").collect()[2][0]


# Create a Row object with a "features" column
query_song_row = Row(features=query_song)

# Create a DataFrame with the Row object
query_song_df = spark.createDataFrame([query_song_row])

# Transform the DataFrame
prediction = model.transform(scaler_model.transform(query_song_df))

# Display the prediction
# prediction.select("prediction").show()

# Display the first 5 files closest to the cluster centers

# Get the cluster centers
centers = model.gaussiansDF.select("mean").collect()

# Get the cluster centers as a list of lists
centers_list = [Vectors.dense(row["mean"].tolist()) for row in centers]  # Convert numpy array to list and then to Vector

# Calculate the distance between each file and the cluster centers
for i in range(len(centers_list)):
    df = df.withColumn(f"distance_to_center_{i}", calculate_distance_udf(col("scaled_features"), lit(i)))

df = df.withColumn("min_distance", least(*[col(f"distance_to_center_{i}") for i in range(len(centers_list))]))


df = df.orderBy("min_distance")

closest_files = df.limit(5).collect()

closest_files_list = [row["file"] for row in closest_files]


# Display the list
print("Closest Files: ",closest_files_list)

# saving the model file 
model.write().overwrite().save("GMM_model")

# Stop SparkSession
spark.stop()
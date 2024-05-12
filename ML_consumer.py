from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, array
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
import pickle

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KNN Music Recommendations") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'song_features_topic6',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Load your pre-trained KNN model
with open('knn_model.pkl', 'rb') as model_file:
    knn = pickle.load(model_file)

# Variables to store messages and file names
features = []
files = []

# Process messages from Kafka
try:
    for message in consumer:
        # Extract features and file name from the message
        message_value = message.value
        feature_vector = message_value['features']
        file_name = message_value['file']

        # Append feature vector and file name to the lists
        features.append(feature_vector)
        files.append(file_name)

        # Check if we have received at least one message to use as query
        if len(features) == 1:
            query_vector = np.array(feature_vector).reshape(1, -1)

        # Assuming we want to train the model after receiving some messages
        if len(features) >= 100:  # Example threshold, adjust as needed
            break

except KeyboardInterrupt:
    print("Stopped by the user.")

finally:
    # Close the consumer connection
    consumer.close()

# Convert features to a numpy array for the KNN model
features = np.array(features)

# Train the KNN model (or you can use the pre-trained model directly)
# Here, we're just using the received features to predict neighbors
knn.fit(features, files)

# Find the k nearest neighbors to the query vector
# Let's assume we're looking for the 3 nearest neighbors
distances, indices = knn.kneighbors(query_vector, n_neighbors=5)

# Print the nearest neighbors and their distances
print("Nearest Neighbors to the first song:")
for i, index in enumerate(indices[0]):
    print(f"{i+1}: {files[index]} with distance {distances[0][i]}")
    
# stop the SparkSession
spark.stop()
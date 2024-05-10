from kafka import KafkaProducer
from pymongo import MongoClient
import json

# Connect to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["audio_features"]
collection = db["features"]

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to retrieve song features from MongoDB
def get_song_features(file_name):
    song_data = collection.find_one({'file_name': file_name})
    return song_data

# Function to send message to Kafka
def send_message(message):
    producer.send('song_features_topic', json.dumps(message).encode('utf-8'))
    print("Message sent to Kafka:", message)

# Main function
def main():
    # Example filename
    file_name = '000002.mp3'
    
    # Retrieve song features from MongoDB
    song_features = get_song_features(file_name)
    
    # Send message to Kafka
    send_message(song_features)

if __name__ == "__main__":
    main()

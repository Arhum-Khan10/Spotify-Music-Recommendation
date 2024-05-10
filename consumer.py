from kafka import KafkaConsumer
import json

# Connect to Kafka
consumer = KafkaConsumer('song_features_topic', bootstrap_servers='localhost:9092', group_id='test-group')

# Main function
def main():
    # Continuously poll for new messages from the Kafka topic
    for message in consumer:
        # Decode message value from bytes to string
        message_value = message.value.decode('utf-8')
        
        # Parse JSON string to Python dictionary
        song_features = json.loads(message_value)
        
        # Print song features
        print("Received song features:")
        print(song_features)
        print()

if __name__ == "__main__":
    main()

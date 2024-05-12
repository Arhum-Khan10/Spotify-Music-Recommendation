from kafka import KafkaProducer
import json
import time
import os
import logging

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Directory containing the songs
songs_directory = '/home/arhum/Downloads/fma_small1/000/'

def send_songs_to_kafka():
    for filename in os.listdir(songs_directory):
        if filename.endswith(".mp3"):
            file_path = os.path.join(songs_directory, filename)
            try:
                # Read the audio file in binary mode
                with open(file_path, 'rb') as audio_file:
                    audio_data = audio_file.read()
                
                # Send file name and audio data to Kafka
                message = {
                    'file_name': filename,
                    'audio_data': audio_data.decode('ISO-8859-1')  # Encode binary data to a string
                }
                producer.send('audio_files_topic1', value=message)
                logging.info(f"Sent {filename} to Kafka")
                time.sleep(0.1)
            except Exception as e:
                logging.error(f"Failed to send {filename} to Kafka due to {e}")

def main():
    send_songs_to_kafka()
    time.sleep(10)

if __name__ == "__main__":
    main()
import os
import json
from kafka import KafkaConsumer
from flask import Flask, Response
from flask import Flask, render_template

app = Flask(__name__)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'audio_files_topic1',
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

@app.route('/play_song')
def play_song():
    file_path = os.path.join('/home/aaqib/Downloads/BDA-Project/BDA-Project/fma_small1/000/', '000002.mp3')
    return stream_audio(file_path)

@app.route('/')
def loading():
    return render_template('loading.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001)
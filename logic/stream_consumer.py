from flask import Flask, Response, render_template, redirect, url_for
from kafka import KafkaConsumer
import os
import json
import csv 

app = Flask(__name__)

# Path to the base directory containing all audio files
base_audio_dir = '/home/aaqib/Downloads/BDA-Project/BDA-Project/fma_small1'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'audio_files_topic1',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

@app.route('/')
def index():
    tracks = []
    with open('fma_metadata/raw_tracks.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            tracks.append({
                'title': row['track_title'],
                'artist': row['artist_name'],
                'album': row['album_title'],
                'file_name': row['track_id']
            })
    return render_template('index1.html', tracks=tracks)


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
                relative_path = os.path.relpath(os.path.join(root, file), base_audio_dir)
                audio_files.append(relative_path)

    return render_template('index1.html', audio_files=audio_files)

@app.route('/play/<path:audio_path>')
def play_audio(audio_path):
    file_path = os.path.join(base_audio_dir, audio_path)
    return stream_audio(file_path)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
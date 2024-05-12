# Project: Personalized Music Streaming Service

## Introduction

The Personalized Music Streaming Service project aims to develop a streamlined alternative to popular music streaming platforms like Spotify. This project incorporates a sophisticated music recommendation system alongside playback and streaming capabilities, delivering real-time suggestions derived from user activity. 

## Dependencies

Before getting started, ensure you have the following dependencies installed:

- [Python](https://www.python.org/downloads/)
- [Flask](https://flask.palletsprojects.com/en/2.0.x/installation/)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [Apache Kafka](https://kafka.apache.org/downloads)
- [MongoDB](https://www.mongodb.com/try/download/community)

- **Python Libraries:**
  - [Kafka-Python](https://github.com/dpkp/kafka-python): `pip install kafka-python`
  - [Pandas](https://pandas.pydata.org/): `pip install pandas`
  - [Librosa](https://librosa.org/doc/main/install.html): `pip install librosa`
  - [NumPy](https://numpy.org/): `pip install numpy`
  - [Scikit-Learn](https://scikit-learn.org/): `pip install scikit-learn`
  - [Joblib](https://joblib.readthedocs.io/en/latest/): `pip install joblib`
  - [Flask](https://flask.palletsprojects.com/en/2.0.x/installation/): `pip install Flask`
  - [PySpark](https://spark.apache.org/downloads.html): `pip install pyspark`
  - [TQDM](https://github.com/tqdm/tqdm): `pip install tqdm`

- **Dataset:**
  - [Free Music Archive (FMA)](https://github.com/mdeff/fma): Download from the GitHub repository.

## Phase #1: Extract, Transform, Load (ETL) Pipeline 

In this phase, we'll create an Extract, Transform, Load (ETL) pipeline utilizing the Free Music Archive (FMA) dataset. The FMA dataset comprises 106,574 tracks, each lasting 30 seconds, spanning 161 unevenly distributed genres. We'll utilize the `fma_large.zip` dataset, along with `fma_metadata.zip` for track details like title, artist, genres, tags, and play counts.

### Tasks:
- Download and preprocess the FMA dataset using Python.
- Extract important features like Mel-Frequency Cepstral Coefficients (MFCC), spectral centroid, or zero-crossing rate.
- Explore normalization, standardization, and dimensionality reduction techniques.
- Store the transformed audio features in MongoDB for scalability and accessibility.

## Phase #2: Music Recommendation Model 

Now that the data is securely stored in MongoDB, we'll train a music recommendation model using Apache Spark. We can leverage Apache Spark’s MLlib machine learning library or explore deep learning methodologies for enhanced accuracy. Algorithms such as collaborative filtering and Approximate Nearest Neighbours (ANN) can be employed in this process.

### Tasks:
- Utilize Apache Spark to train the recommendation model.
- Evaluate the model using different metrics and perform hyperparameter tuning.
- Assess the model's performance and accuracy.

## Phase #3: Deployment 

In this phase, we'll deploy the trained model onto a web application with streaming capabilities. We'll leverage frameworks like Flask or Django to develop an interactive music streaming web application. Apache Kafka will be used to dynamically generate music recommendations in real-time, based on user activity and historical playback data.

### Tasks:
- Develop an interactive music streaming web application with Flask or Django.
- Utilize Apache Kafka for real-time music recommendation generation.
- Implement a user-friendly web interface for seamless navigation and usage.

## Folder Structure

- **ML_Logic**: Contains machine learning logic for music recommendation.
- **Streaming_Logic**: Contains logic for real-time music streaming recommendations.
- **producer.py**: Generates music recommendations in real-time using Apache Kafka.
- **music_app.py**: Main application file.
- **static**: Contains static files for the web application (e.g., CSS, JavaScript).
- **test_logic**: Contains test logic for unit testing.

## Team

Meet the dedicated individuals who contributed to this project:

- **Ammar Khasif**: [GitHub](https://github.com/ammar-kashif)
- **Arhum Khan**: [GitHub](https://github.com/Arhum-Khan10)
- **Aaqib Ahmed Nazir**: [GitHub](https://github.com/aaqib-ahmed-nazir)

## Additional Information

This project aims to redefine the music streaming experience by delivering personalized recommendations and real-time streaming capabilities. 

**Let the music play! 🎶**

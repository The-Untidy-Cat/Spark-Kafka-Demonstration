from flask import Flask
from kafka import create_topic, list_topics
app = Flask(__name__)

create_topic("devices")

@app.route("/")
def hello_world():
    topics = list_topics()
    return f"Hello, World! {topics}"
from flask import Flask, request, jsonify
from datetime import datetime
import random
import json
from kafka import create_topic_if_not_exist, send_message
app = Flask(__name__)

create_topic_if_not_exist("logs")


@app.route("/")
def index():
    user_agent = request.headers.get("User-Agent")
    broswer = user_agent.split("/")[0]
    timestamp = datetime.now()
    status_code = random.choice([200, 400, 500])
    data = {
        "browser": broswer,
        "status_code": status_code,
        "datetime": timestamp.isoformat()
    }
    json_data = json.dumps(data).encode('utf-8')
    send_message("logs", json_data)
    return jsonify(data=data), status_code

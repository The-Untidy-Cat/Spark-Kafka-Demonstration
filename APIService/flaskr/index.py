from flask import Flask, request, jsonify
from datetime import datetime
import random
import json
from kafka import create_topic_if_not_exist, send_message
from user_agents import parse

app = Flask(__name__)

create_topic_if_not_exist("logs")


@app.route("/")
def index():
    ua_client = request.headers.get("User-Agent")
    ua_parsed = parse(ua_client)
    broswer = ua_parsed.browser.family
    os = ua_parsed.os.family
    device = ua_parsed.device.family
    is_bot = ua_parsed.is_bot
    timestamp = datetime.now()
    status_code = random.choice([200, 400, 500])
    data = {
        "browser": broswer,
        "os": os,
        "device": device,
        "is_bot": is_bot,
        "status_code": status_code,
        "datetime": timestamp.isoformat()
    }
    json_data = json.dumps(data).encode('utf-8')
    send_message("logs", json_data)
    return jsonify(data=data), status_code

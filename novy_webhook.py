from flask import Flask, request, abort
import base64
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# Set your expected Basic Auth credentials
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")


def check_basic_auth(auth_header):
    if not auth_header:
        return False

    try:
        scheme, b64_credentials = auth_header.split(" ")
        if scheme != "Basic":
            return False

        credentials = base64.b64decode(b64_credentials).decode("utf-8")
        input_user, input_pass = credentials.split(":", 1)
        return input_user == USERNAME and input_pass == PASSWORD
    except Exception:
        return False

@app.route("/webhook", methods=["POST"])
def webhook():
    auth_header = request.headers.get("Authorization")

    if not check_basic_auth(auth_header):
        abort(401, description="Unauthorized")

    data = request.json
    print("✅ Received webhook payload:", data)

    return {"status": "received"}, 200

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

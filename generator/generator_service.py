
from flask import Flask, request, jsonify
import random

app = Flask(__name__)

@app.route('/generate', methods=['POST'])
def generate():
    model_name = request.json.get('model_name')
    viewer_id = request.json.get('viewer_id')
    result = random.randint(1, 100)  # Generate a random number between 1 and 100
    return jsonify({"reason": model_name, "result": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

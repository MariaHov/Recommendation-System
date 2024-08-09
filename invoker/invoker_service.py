from flask import Flask, request, jsonify
import requests
from cachetools import TTLCache
import redis
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize the Flask application
app = Flask(__name__)

# Local cache setup: 3 keys max, with a TTL of 10 seconds
local_cache = TTLCache(maxsize=3, ttl=10)

# Redis cache setup
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

def runcascade(viewer_id):
    model_names = ['model1', 'model2', 'model3', 'model4', 'model5']

    def make_request(model_name):
        response = requests.post('http://generator:5000/generate', json={"model_name": model_name, "viewer_id": viewer_id})
        return response.json()

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_model = {executor.submit(make_request, model_name): model_name for model_name in model_names}

        for future in as_completed(future_to_model):
            model_name = future_to_model[future]
            try:
                data = future.result()
                results.append(data)
            except Exception as exc:
                print(f'{model_name} generated an exception: {exc}')

    return results

@app.route('/recommend', methods=['POST'])
def recommend():
    viewer_id = request.json.get('viewer_id')
    if viewer_id in local_cache:
        return jsonify(local_cache[viewer_id])
    elif redis_client.exists(viewer_id):
        redis_data = json.loads(redis_client.get(viewer_id))
        local_cache[viewer_id] = redis_data
        return jsonify(redis_data)
    else:
        result = runcascade(viewer_id)
        local_cache[viewer_id] = result
        redis_client.setex(viewer_id, 60, json.dumps(result))
        return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)

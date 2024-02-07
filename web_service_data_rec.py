from flask import Flask, render_template
from redis import Redis
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # This will enable CORS for all routes
redis = Redis(host='localhost', port=6379)

# Route to render the dashboard template
@app.route('/')
def dashboard():
    return render_template('dashboard.html')

# Route to fetch data from Redis and return as JSON
@app.route('/data')
def get_data():
    data = []
    try:
        while True:
            raw_data = redis.blpop('financial_data', timeout=10)
            if raw_data:
                data.append(json.loads(raw_data[-1]))
                print("Received data:", data[-1])
                return json.dumps(data)
    except Exception as e:
        print("Redis Error:", e)
    
    

if __name__ == '__main__':
    app.run(debug=True)

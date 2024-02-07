import time
import json
from redis import Redis
import random

redis = Redis(host='localhost', port=6379)

def send_data():
    while True:
        # Simulated financial data
        data = {
            'timestamp': int(time.time()),
            'price': random.uniform(100, 200)  # Simulated price between 100 and 200
        }
        # Send data to Redis
        print(f"send data: {data}")
        redis.rpush('financial_data', json.dumps(data))
        time.sleep(1)  # Sending data every second

if __name__ == '__main__':
    send_data()



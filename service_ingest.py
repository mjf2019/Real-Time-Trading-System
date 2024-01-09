from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/ingest', methods=['POST'])
def ingest():
    try:
        data = request.json  # Assumes the incoming data is JSON
        print(f"Received data: {data}")

        return jsonify(data), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

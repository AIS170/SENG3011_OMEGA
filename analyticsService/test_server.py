from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/receive-data", methods=["POST"])
def receive_data():
    try:
        received_data = request.get_json()
        
        if not received_data:
            return jsonify({"error": "No data received"}), 400
        
        # Log received data for debugging
        print("📥 Received Forecast Data:")
        print(received_data)

        return jsonify({"status": "success", "message": "Data received successfully"}), 200

    except Exception as e:
        print({"error": str(e)})
        return jsonify({"error": str(e)}), 500
    
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=True)

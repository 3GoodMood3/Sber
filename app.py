from flask import Flask, request, jsonify
from confluent_kafka import Producer

app = Flask(__name__)

kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

@app.route('/send', methods=['POST'])
def send_to_kafka():
    if request.content_type != 'application/json':
        return jsonify({'status': 'error', 'message': 'Не поддерживаймый тип'}), 415

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({'status': 'error', 'message': 'Требуются данные в формате JSON'}), 400

    try:
        kafka_producer.produce('alexey', value=str(data))
        kafka_producer.flush()
        return jsonify({'status': 'Успешно'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000)

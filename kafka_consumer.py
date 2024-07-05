from confluent_kafka import Consumer, KafkaException

kafka_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
})

kafka_consumer.subscribe(['alexey'])

try:
    while True:
        message = kafka_consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(message.error())
                break

        print(f'Получено сообщение: {message.value().decode("utf-8")}')

except KeyboardInterrupt:
    print('Прервано пользователем')

finally:
    kafka_consumer.close()
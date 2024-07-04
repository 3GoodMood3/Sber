from confluent_kafka import Consumer, KafkaException

# Настройка потребителя Kafka
kafka_consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
})

# Подписка на топик
kafka_consumer.subscribe(['alexey'])

try:
    while True:
        # Получение сообщения из Kafka
        message = kafka_consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaException._PARTITION_EOF:
                # Конец топика был достигнут
                continue
            else:
                print(message.error())
                break

        # Сообщение было получено
        print(f'Получено сообщение: {message.value().decode("utf-8")}')

except KeyboardInterrupt:
    print('Прервано пользователем')

finally:
    # Закрытие потребителя
    kafka_consumer.close()
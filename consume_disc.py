from kafka import KafkaConsumer
import json

def consume_messages(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Start from the earliest message
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize JSON
    )

    print(f"Consuming messages from topic '{topic}'...")
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    consume_messages('discord-messages')

import os
import requests
import json
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

def retrieve_messages(channel_id, limit=100):
    auth_key = os.getenv('DISCORD_AUTH_KEY')
    if not auth_key:
        raise ValueError("Authorization key is not set in environment variables.")
    
    headers = {
        'authorization': auth_key
    }
    url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
    params = {
        'limit': 50  # Max number of messages per request
    }

    all_messages = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch messages: {response.status_code}")
            break

        messages = response.json()
        if not messages:
            break  # Stop if no more messages are returned

        all_messages.extend(messages)

        # Prepare for the next batch
        params['before'] = messages[-1]['id']  # Get messages before the last message

        if len(all_messages) >= limit:
            break  # Stop once we've collected the desired number of messages

    return all_messages

def send_to_kafka(producer, topic, messages, sent_ids):
    count = 0
    for message in messages:
        if message['id'] in sent_ids:
            continue

        payload = {
            "author": message["author"],
            "content": message["content"],
            "timestamp": message["timestamp"]
        }

        producer.send(topic, value=json.dumps(payload).encode('utf-8'))
        count += 1
        sent_ids.add(message['id'])

    print(f"Sent {count} messages to Kafka topic '{topic}'.")

def main():
    # List of Discord channel IDs
    channel_ids = ['172018499005317120', '174075418410876928', '863074988440158258', '904421116343382116']
    kafka_topic = 'discord-ml'

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Track sent message IDs for each channel
    sent_ids_map = {channel_id: set() for channel_id in channel_ids}

    try:
        while True:
            for channel_id in channel_ids:
                print(f"Processing channel: {channel_id}")
                
                # Retrieve messages for this channel
                messages = retrieve_messages(channel_id, limit=4000)

                # Send messages to Kafka for this channel
                if messages:
                    send_to_kafka(producer, kafka_topic, messages, sent_ids_map[channel_id])
                
            # Wait before polling again
            time.sleep(10)

    except KeyboardInterrupt:
        print("Stopping the producer.")
    finally: 
        producer.close()

if __name__ == "__main__":
    main()

from kafka import KafkaConsumer
import json
import csv

def consume_reddit_posts_to_csv(topic, output_file):
    # Create a KafkaConsumer instance
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',  # Start consuming from the earliest offset
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize message value as JSON
    )

    print(f"Consuming messages from topic '{topic}' and saving to '{output_file}' in CSV format...")

    # Initialize the CSV file with headers
    first_message = True
    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            csv_writer = None  # Declare here to define in loop
            
            for message in consumer:
                msg = message.value  # Extract the JSON content from the Kafka message

                print(f"Received message: {msg}")  # Log the consumed message (Optional)

                # If it's the first message, initialize the CSV writer with headers
                if first_message:
                    # Defining headers based on Reddit post data
                    fieldnames = ["title", "content", "upvotes", "comments_count", "top_comments", "created", "author", "url"]
                    csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
                    csv_writer.writeheader()  # Write headers to the file
                    first_message = False

                # Write the Reddit post data as a row in the CSV file
                csv_writer.writerow({
                    "title": msg.get("title"),
                    "content": msg.get("content"),
                    "upvotes": msg.get("upvotes"),
                    "comments_count": msg.get("comments_count"),
                    "top_comments": "; ".join(msg.get("top_comments", [])),  # Join the comments list into a string
                    "created": msg.get("created"),
                    "author": msg.get("author"),
                    "url": msg.get("url")
                })
                print("Written")
    except KeyboardInterrupt:
        # Gracefully handle a user interruption (Ctrl+C)
        print("\nStopping consumer...")
    finally:
        consumer.close()  # Close the Kafka consumer
        print("Consumer closed.")

if __name__ == "__main__":
    # Specify the Kafka topic and the output CSV file
    consume_reddit_posts_to_csv('reddit-ml', 'reddit_posts.csv')

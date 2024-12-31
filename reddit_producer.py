import praw
import pandas as pd
import json
from kafka import KafkaProducer


# Initialize Reddit API
reddit = praw.Reddit()

# Set Kafka topic and producer configuration
kafka_topic = "reddit-ml"
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Define subreddits and keywords 
subreddits = ["technology", "technews", "Futurology", "startups"]
keywords = ["innovation", "AI", "machine learning", "blockchain", "cloud", "agile"]

def fetch_reddit_posts(subreddits, keywords, limit=100):
    data = []
    for subreddit in subreddits:
        print(f"Fetching posts from r/{subreddit}...")
        for post in reddit.subreddit(subreddit).new(limit=limit):
            # Filter posts by keywords
            if any(keyword.lower() in post.title.lower() or 
                   (post.selftext and keyword.lower() in post.selftext.lower()) 
                   for keyword in keywords):
                # Fetch top-level comments
                comments = []
                post.comments.replace_more(limit=0)  # Fetch only top-level comments
                for comment in post.comments:
                    comments.append(comment.body)

                data.append({
                    "Title": post.title,
                    "Content": post.selftext if post.selftext else "No Content",
                    "Upvotes": post.score,
                    "Comments_Count": post.num_comments,
                    "Top_Comments": comments[:10],
                    "Created": pd.to_datetime(post.created_utc, unit="s"),
                    "Author": post.author.name if post.author else "Unknown",
                    "URL": post.url
                })

    return data

def send_to_kafka(producer, topic, posts):
    count = 0
    for post in posts:
        # Prepare the payload
        payload = {
            "title": post["Title"],
            "content": post["Content"],
            "upvotes": post["Upvotes"],
            "comments_count": post["Comments_Count"],
            "top_comments": post["Top_Comments"],
            "created": post["Created"].isoformat(),
            "author": post["Author"],
            "url": post["URL"]
        }

        # Send data to Kafka
        producer.send(topic, value=json.dumps(payload).encode("utf-8"))
        count += 1

    print(f"Sent {count} Reddit posts to Kafka topic '{topic}'.")

def main():
    try:
        while True:
            # Fetch Reddit posts
            reddit_data = fetch_reddit_posts(subreddits, keywords)
            if reddit_data:
                # Send fetched posts to Kafka
                send_to_kafka(producer, kafka_topic, reddit_data)
            else:
                print("No relevant Reddit posts found at the moment.")
            
            # Optional: Sleep before re-fetching
            # time.sleep(10)

    except KeyboardInterrupt:
        print("Stopping the producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

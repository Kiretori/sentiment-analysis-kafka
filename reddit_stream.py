import praw
import pandas as pd



# Reddit API
reddit = praw.Reddit()

# Define subreddits and keywords 
subreddits = ["technology", "technews", "Futurology", "startups"]
keywords = ["innovation", "AI", "machine learning", "blockchain", "cloud", "agile"]

def fetch_reddit_posts(subreddits, keywords, limit=250):
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
    return pd.DataFrame(data)

# display data
reddit_data = fetch_reddit_posts(subreddits, keywords)
print(f"Fetched {len(reddit_data)} relevant posts.")

reddit_data.to_csv("reddit_posts1.csv", index=False)
print("Data saved : 'reddit_posts1.csv'")
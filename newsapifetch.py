import requests
import pandas as pd

# NewsAPI Configuration
API_KEY = ""
BASE_URL = "https://newsapi.org/v2/everything"

# Search Parameters
KEYWORDS = [
    # General Technology
    "technology", "innovation", "digital transformation",

    # Artificial Intelligence & Machine Learning
    "AI", "machine learning", "deep learning", "natural language processing",

    # Blockchain & Cryptocurrencies
    "blockchain", "cryptocurrency", "smart contracts",

    # Cloud Computing
    "cloud computing", "Kubernetes", "serverless architecture",

    # Software Development Practices
    "DevOps", "agile", "continuous integration",

    # Big Data & Analytics
    "big data", "data science", "predictive analytics",

    # Cybersecurity
    "cybersecurity", "ransomware", "threat detection",

    # Emerging Technologies
    "quantum computing", "augmented reality", "virtual reality", "robotics",

    # Popular Topics & Buzzwords
    "startups", "venture capital", "open source", "scalability"
]


LANGUAGE = "en"
SORT_BY = "relevancy"  # Options: relevancy, popularity, publishedAt
SOURCES = (
    "techcrunch,wired,the-verge,ars-technica,reuters,engadget,"
    "venturebeat,cnet,zdnet,gizmodo,business-insider,mashable,"
    "bbc-news,forbes,financial-times,economist,mit-technology-review"
) # Tech-oriented sources
PAGE_SIZE = 20  # Max results per page
PAGE = 1  # Page number for results

# Function to fetch articles from NewsAPI
def fetch_news_articles(keywords, language="en", sort_by="relevancy", sources=None, page_size=20, page=1):
    articles = []
    for keyword in keywords:
        print(f"Fetching articles for keyword: {keyword}")
        params = {
            "q": keyword,
            "language": language,
            "sortBy": sort_by,
            "sources": sources,
            "pageSize": page_size,
            "page": page,
            "apiKey": API_KEY,
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            if "articles" in data:
                articles.extend(data["articles"])
        else:
            print(f"Error fetching articles for keyword {keyword}: {response.status_code}")
    return articles

def filter_articles(articles):
    filtered_articles = []
    for article in articles:
        # Check for required fields and exclude removed articles
        if all([
            article.get("title"),  
            article.get("description"),  
            article.get("content"),  
            article.get("source", {}).get("name"),  
            article.get("url"),  
        ]) and not any([
            article.get("title") == "[Removed]",  
            article.get("description") == "[Removed]",  
            article.get("content") == "[Removed]",  
        ]):
            filtered_articles.append(article)
    return filtered_articles

# Fetch articles
raw_articles = fetch_news_articles(KEYWORDS, LANGUAGE, SORT_BY, SOURCES, PAGE_SIZE, PAGE)

# Filter the articles
filtered_articles = filter_articles(raw_articles)

# Process and save results
if filtered_articles:
    articles_df = pd.DataFrame([{
        "Title": article["title"],
        "Description": article["description"],
        "Content": article.get("content", "No content available"),
        "Published At": article["publishedAt"],
        "Author": article.get("author", "Unknown"),
        "Source": article["source"]["name"],
        "URL": article["url"]
    } for article in filtered_articles])

    print(f"Fetched and filtered {len(filtered_articles)} articles.")
    articles_df.to_csv("newsapiorg.csv", index=False)
    print("Articles saved to 'newsapiorg.csv'")
else:
    print("No articles found.")

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import pandas as pd
from kafka.consumer.fetcher import ConsumerRecord

# Test produce_disc.py
@pytest.fixture
def mock_kafka_producer():
    with patch('kafka.KafkaProducer') as mock_producer:
        producer_instance = Mock()
        mock_producer.return_value = producer_instance
        yield producer_instance

@pytest.fixture
def mock_discord_response():
    return [
        {
            'id': '123456789',
            'author': {'username': 'test_user', 'id': '11111'},
            'content': 'Test message',
            'timestamp': '2024-01-01T00:00:00.000Z'
        },
        {
            'id': '987654321',
            'author': {'username': 'test_user2', 'id': '22222'},
            'content': 'Another test message',
            'timestamp': '2024-01-01T00:00:00.000Z'
        }
    ]

def test_retrieve_messages(mock_discord_response):
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_discord_response
        
        from produce_disc import retrieve_messages
        messages = retrieve_messages('test_channel_id', limit=2)
        
        assert len(messages) == 2
        assert messages[0]['id'] == '123456789'
        assert messages[1]['id'] == '987654321'

def test_send_to_kafka_discord(mock_kafka_producer):
    from produce_disc import send_to_kafka
    
    messages = [
        {
            'id': '123',
            'author': {'username': 'test_user'},
            'content': 'test message',
            'timestamp': '2024-01-01T00:00:00.000Z'
        }
    ]
    
    sent_ids = set()
    send_to_kafka(mock_kafka_producer, 'test-topic', messages, sent_ids)
    
    assert mock_kafka_producer.send.called
    assert '123' in sent_ids

# Test reddit_producer.py
@pytest.fixture
def mock_reddit_post():
    post = MagicMock()
    post.title = "Test Post"
    post.selftext = "Test Content"
    post.score = 100
    post.num_comments = 10
    post.created_utc = 1640995200  # 2022-01-01 00:00:00
    post.author.name = "test_author"
    post.url = "https://reddit.com/test"
    
    comment = MagicMock()
    comment.body = "Test comment"
    post.comments.__iter__.return_value = [comment]
    
    return post

def test_fetch_reddit_posts(mock_reddit_post):
    with patch('praw.Reddit') as mock_reddit:
        mock_subreddit = MagicMock()
        mock_subreddit.new.return_value = [mock_reddit_post]
        mock_reddit.return_value.subreddit.return_value = mock_subreddit
        
        from reddit_producer import fetch_reddit_posts
        posts = fetch_reddit_posts(['test'], ['test'], limit=1)
        
        assert len(posts) == 1
        assert posts[0]['Title'] == "Test Post"
        assert posts[0]['Content'] == "Test Content"
        assert posts[0]['Author'] == "test_author"

def test_send_to_kafka_reddit(mock_kafka_producer):
    from reddit_producer import send_to_kafka
    
    posts = [{
        'Title': 'Test Post',
        'Content': 'Test Content',
        'Upvotes': 100,
        'Comments_Count': 10,
        'Top_Comments': ['Test comment'],
        'Created': pd.Timestamp('2024-01-01'),
        'Author': 'test_author',
        'URL': 'https://reddit.com/test'
    }]
    
    send_to_kafka(mock_kafka_producer, 'test-topic', posts)
    
    assert mock_kafka_producer.send.called

# Test consumers
@pytest.fixture
def mock_kafka_consumer():
    with patch('kafka.KafkaConsumer') as mock_consumer:
        consumer_instance = MagicMock()  # Changed from Mock to MagicMock
        consumer_instance.__iter__.return_value = []  # Initialize the iterator
        mock_consumer.return_value = consumer_instance
        yield consumer_instance

@pytest.fixture
def mock_discord_message():
    return ConsumerRecord(
        topic='discord-ml',
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=json.dumps({
            'author': {'username': 'test_user'},
            'content': 'test message',
            'timestamp': '2024-01-01T00:00:00.000Z'
        }).encode('utf-8'),
        headers=[],
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=0,
        serialized_header_size=0
    )

@pytest.fixture
def mock_reddit_message():
    return ConsumerRecord(
        topic='reddit-ml',
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=json.dumps({
            'title': 'Test Post',
            'content': 'Test Content',
            'upvotes': 100,
            'comments_count': 10,
            'top_comments': ['Test comment'],
            'created': '2024-01-01T00:00:00',
            'author': 'test_author',
            'url': 'https://reddit.com/test'
        }).encode('utf-8'),
        headers=[],
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=0,
        serialized_header_size=0
    )

def test_consume_discord_messages(mock_kafka_consumer, mock_discord_message, tmp_path):
    mock_kafka_consumer.__iter__.return_value = [mock_discord_message]
    output_file = tmp_path / "test_discord_output.csv"
    
    from consume_disc import consume_messages_to_csv
    consume_messages_to_csv('discord-ml', str(output_file))
    
    assert output_file.exists()
    with open(output_file, 'r') as f:
        content = f.read()
        assert 'test_user' in content
        assert 'test message' in content

def test_consume_reddit_messages(mock_kafka_consumer, mock_reddit_message, tmp_path):
    mock_kafka_consumer.__iter__.return_value = [mock_reddit_message]
    output_file = tmp_path / "test_reddit_output.csv"
    
    from reddit_consumer import consume_reddit_posts_to_csv
    consume_reddit_posts_to_csv('reddit-ml', str(output_file))
    
    assert output_file.exists()
    with open(output_file, 'r') as f:
        content = f.read()
        assert 'Test Post' in content
        assert 'test_author' in content
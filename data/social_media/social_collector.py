import sqlite3
import os
import requests
import praw
import feedparser
from datetime import datetime
from dotenv import load_dotenv
from email.utils import parsedate_to_datetime

load_dotenv()

def normalize_date(date_str):
    """Convert any date format to YYYY-MM-DD HH:MM:SS format"""
    if not date_str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        if 'GMT' in date_str or 'UTC' in date_str:
            dt = parsedate_to_datetime(date_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def init_database():
    db_path = "data/social_media/social.db"
    
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS social_posts (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                title TEXT NOT NULL,
                content TEXT,
                url TEXT UNIQUE,
                source TEXT,
                published_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    return db_path

def store_post(db_path, ticker, title, content, url, source, published_at):
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO social_posts 
                (ticker, title, content, url, source, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, title, content, url, source, published_at))
            return True
        except:
            return False

def collect_reddit_posts(ticker, latest_date=None):
    posts = []
    try:
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'financial_collector')
        )
        
        # Search in financial subreddits
        subreddits = ['stocks', 'investing', 'SecurityAnalysis', 'ValueInvesting', 'StockMarket', 'wallstreetbets']
        
        for sub_name in subreddits:
            try:
                subreddit = reddit.subreddit(sub_name)
                for submission in subreddit.search(ticker, limit=50):
                    post_date = normalize_date(datetime.fromtimestamp(submission.created_utc).isoformat())
                    # Skip if post is older than latest_date
                    if latest_date and post_date <= latest_date:
                        continue
                        
                    if ticker.upper() in submission.title.upper() or ticker.upper() in submission.selftext.upper():
                        posts.append((
                            submission.title,
                            submission.selftext,
                            f"https://reddit.com{submission.permalink}",
                            f'Reddit ({sub_name})',
                            post_date
                        ))
            except:
                continue
    except:
        pass
    
    return posts

def collect_rss_feeds(ticker, latest_date=None):
    posts = []
    
    # Nitter RSS feeds for Twitter-like content
    nitter_instances = [
        'nitter.net',
        'nitter.it'
    ]
    
    for instance in nitter_instances:
        try:
            # Search for ticker mentions
            rss_url = f"https://{instance}/search/rss?f=tweets&q=${ticker}"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                post_date = normalize_date(entry.get('published', ''))
                # Skip if post is older than latest_date
                if latest_date and post_date <= latest_date:
                    continue
                    
                if ticker.upper() in entry.get('title', '').upper():
                    posts.append((
                        entry.get('title', ''),
                        entry.get('description', ''),
                        entry.get('link', ''),
                        f'Nitter-{instance}',
                        post_date
                    ))
            
            # If we got data from this instance, break (don't try others)
            if len(feed.entries) > 0:
                break
                
        except:
            continue
    
    return posts

def collect_stocktwits_posts(ticker, latest_date=None):
    posts = []
    try:
        # StockTwits API
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            for message in data.get('messages', []):
                post_date = normalize_date(message.get('created_at', ''))
                # Skip if post is older than latest_date
                if latest_date and post_date <= latest_date:
                    continue
                    
                posts.append((
                    message.get('body', ''),
                    message.get('body', ''),
                    f"https://stocktwits.com/{message.get('user', {}).get('username')}/message/{message.get('id')}",
                    'StockTwits',
                    post_date
                ))
    except:
        pass
    
    return posts

def get_latest_published_date(db_path, ticker):
    """Get the latest published_at date for a ticker from database"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(published_at) FROM social_posts 
                WHERE ticker = ?
            """, (ticker,))
            result = cursor.fetchone()
            return result[0] if result[0] else None
    except:
        return None

def collect_social_for_ticker(ticker):
    print(f"Starting social media collection for {ticker}...")
    db_path = init_database()
    
    # Check for existing data
    latest_date = get_latest_published_date(db_path, ticker)
    if latest_date:
        print(f"Found existing data. Latest post: {latest_date}")
        print(f"Fetching posts after: {latest_date}")
    else:
        print("No existing data found. Fetching all available posts.")
    
    print(f"{'-'*50}")
    print(f"Source \t\t\tPost Count")
    
    all_posts = []
    
    # Reddit posts
    reddit_posts = collect_reddit_posts(ticker, latest_date)
    all_posts.extend(reddit_posts)
    print(f"Reddit  \t\t{len(reddit_posts)}")
    
    # RSS feeds (Nitter)
    rss_posts = collect_rss_feeds(ticker, latest_date)
    all_posts.extend(rss_posts)
    print(f"RSS Feeds\t\t{len(rss_posts)}")
    
    # StockTwits posts
    stocktwits_posts = collect_stocktwits_posts(ticker, latest_date)
    all_posts.extend(stocktwits_posts)
    print(f"StockTwits\t\t{len(stocktwits_posts)}")
    
    # Store all posts
    stored = 0
    for title, content, url, source, published_at in all_posts:
        if store_post(db_path, ticker, title, content, url, source, published_at):
            stored += 1
    
    print(f"{'-'*50}")
    print(f"Collected:\t{len(all_posts)} posts")
    print(f"Stored: \t{stored} posts")
    return stored


if __name__ == "__main__":
    collect_social_for_ticker("GOOGL")
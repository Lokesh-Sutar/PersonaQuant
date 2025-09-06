import sqlite3
import os
import requests
import praw
import feedparser
from datetime import datetime
from dotenv import load_dotenv
from email.utils import parsedate_to_datetime
from typing import Optional, List, Tuple
import logging

load_dotenv()

def normalize_date(date_str: Optional[str]) -> str:
    """Convert various date formats to standardized YYYY-MM-DD HH:MM:SS format.
    
    Args:
        date_str: Date string in various formats (GMT, UTC, ISO, etc.) or None.
        
    Returns:
        Standardized date string in YYYY-MM-DD HH:MM:SS format.
        Returns current datetime if parsing fails or input is None.
    """
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
    except (ValueError, TypeError, AttributeError) as e:
        logging.debug(f"Date parsing failed for '{date_str}': {e}")
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def init_database() -> str:
    """Initialize the social media database and create tables if they don't exist.
    
    Returns:
        Path to the database file.
    """
    db_path = "data/social_media/social.db"
    
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
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
    except (sqlite3.Error, OSError) as e:
        logging.error(f"Database initialization failed: {e}")
        raise

def store_post(db_path: str, ticker: str, title: str, content: str, url: str, source: str, published_at: str) -> bool:
    """Store a social media post in the database.
    
    Args:
        db_path: Path to the SQLite database file.
        ticker: Stock ticker symbol.
        title: Post title or main content.
        content: Post content/body.
        url: Post URL.
        source: Social media source name.
        published_at: Publication date in YYYY-MM-DD HH:MM:SS format.
        
    Returns:
        True if post was stored successfully, False otherwise.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO social_posts 
                (ticker, title, content, url, source, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, title, content, url, source, published_at))
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to store post '{title}': {e}")
        return False

def collect_reddit_posts(ticker: str, latest_date: Optional[str] = None) -> List[Tuple[str, str, str, str, str]]:
    """Collect posts from Reddit financial subreddits mentioning the ticker.
    
    Args:
        ticker: Stock ticker symbol to search for.
        latest_date: Only collect posts newer than this date.
        
    Returns:
        List of tuples containing (title, content, url, source, published_at).
    """
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
                    try:
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
                    except (AttributeError, TypeError, OSError) as e:
                        logging.debug(f"Failed to process Reddit post: {e}")
                        continue
            except (praw.exceptions.RedditAPIException, requests.RequestException) as e:
                logging.warning(f"Reddit subreddit {sub_name} error: {e}")
                continue
    except (praw.exceptions.PRAWException, requests.RequestException) as e:
        logging.debug(f"Reddit API initialization failed: {e}")
    
    return posts

def collect_rss_feeds(ticker: str, latest_date: Optional[str] = None) -> List[Tuple[str, str, str, str, str]]:
    """Collect posts from RSS feeds (Nitter instances) mentioning the ticker.
    
    Args:
        ticker: Stock ticker symbol to search for.
        latest_date: Only collect posts newer than this date.
        
    Returns:
        List of tuples containing (title, content, url, source, published_at).
    """
    posts = []
    
    # Multiple RSS feeds for social content
    social_feeds = [
        # Nitter instances
        (f'https://nitter.net/search/rss?f=tweets&q=${ticker}', 'Nitter-net'),
        (f'https://nitter.it/search/rss?f=tweets&q=${ticker}', 'Nitter-it'),
        # Reddit RSS feeds
        (f'https://www.reddit.com/r/stocks/search.rss?q={ticker}&restrict_sr=1&sort=new', 'Reddit-RSS'),
        (f'https://www.reddit.com/r/investing/search.rss?q={ticker}&restrict_sr=1&sort=new', 'Reddit-Invest'),
        (f'https://www.reddit.com/r/SecurityAnalysis/search.rss?q={ticker}&restrict_sr=1&sort=new', 'Reddit-Analysis')
    ]
    
    for feed_url, source in social_feeds:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; SocialCollector/1.0)'}
            response = requests.get(feed_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            feed = feedparser.parse(response.content)
            if hasattr(feed, 'bozo') and feed.bozo and len(feed.entries) == 0:
                logging.debug(f"RSS parsing failed for {source}: {feed.bozo_exception}")
                print(f"{source}\t\t0 (Parse Error)")
                continue
            
            source_count = 0
            for entry in feed.entries:
                try:
                    post_date = normalize_date(entry.get('published', ''))
                    # Skip if post is older than latest_date
                    if latest_date and post_date <= latest_date:
                        continue
                        
                    # Check if ticker is mentioned in title or description
                    text = f"{entry.get('title', '')} {entry.get('description', '')}".upper()
                    if ticker.upper() in text:
                        posts.append((
                            entry.get('title', ''),
                            entry.get('description', ''),
                            entry.get('link', ''),
                            source,
                            post_date
                        ))
                        source_count += 1
                except (AttributeError, TypeError) as e:
                    logging.debug(f"Failed to process RSS entry from {source}: {e}")
                    continue
            print(f"{source}\t\t{source_count}")
        except requests.RequestException as e:
            logging.debug(f"RSS feed {source} failed: {e}")
            print(f"{source}\t\t0 (Network Error)")
            continue
        except Exception as e:
            logging.debug(f"RSS error for {source}: {e}")
            print(f"{source}\t\t0 (Error)")
            continue
    
    return posts

def collect_stocktwits_posts(ticker: str, latest_date: Optional[str] = None) -> List[Tuple[str, str, str, str, str]]:
    """Collect posts from StockTwits API for the specified ticker.
    
    Args:
        ticker: Stock ticker symbol to get posts for.
        latest_date: Only collect posts newer than this date.
        
    Returns:
        List of tuples containing (title, content, url, source, published_at).
    """
    posts = []
    
    # Try StockTwits API first
    try:
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; SocialCollector/1.0)'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        if 'errors' in data:
            logging.debug(f"StockTwits API error: {data['errors']}")
        else:
            for message in data.get('messages', []):
                try:
                    post_date = normalize_date(message.get('created_at', ''))
                    if latest_date and post_date <= latest_date:
                        continue
                        
                    user_info = message.get('user', {})
                    username = user_info.get('username', 'unknown')
                    message_id = message.get('id', '')
                    
                    posts.append((
                        message.get('body', ''),
                        message.get('body', ''),
                        f"https://stocktwits.com/{username}/message/{message_id}",
                        'StockTwits',
                        post_date
                    ))
                except (KeyError, AttributeError, TypeError) as e:
                    logging.debug(f"Failed to process StockTwits message: {e}")
                    continue
    except requests.RequestException as e:
        logging.debug(f"StockTwits API blocked or failed: {e}")
        
        # Fallback: Try alternative financial discussion sources
        try:
            fallback_url = f"https://finviz.com/quote.ashx?t={ticker}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(fallback_url, headers=headers, timeout=10)
            if response.status_code == 200:
                logging.debug(f"Fallback source accessible for {ticker}")
        except:
            pass
    except (ValueError, KeyError) as e:
        logging.debug(f"StockTwits response parsing failed: {e}")
    
    return posts

def get_latest_published_date(db_path: str, ticker: str) -> Optional[str]:
    """Get the most recent publication date for posts of a specific ticker.
    
    Args:
        db_path: Path to the SQLite database file.
        ticker: Stock ticker symbol to query.
        
    Returns:
        Latest publication date string or None if no posts found.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(published_at) FROM social_posts 
                WHERE ticker = ?
            """, (ticker,))
            result = cursor.fetchone()
            return result[0] if result[0] else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get latest date for {ticker}: {e}")
        return None

def collect_social_for_ticker(ticker: str) -> int:
    """Collect social media posts for a specific stock ticker from multiple sources.
    
    Fetches posts from Reddit, RSS feeds (Nitter), and StockTwits. Only collects
    posts newer than the latest existing post in the database.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
        
    Returns:
        Number of posts successfully stored in the database.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"Starting social media collection for {ticker}...")
    
    try:
        db_path = init_database()
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return 0
    
    # Check for existing data
    latest_date = get_latest_published_date(db_path, ticker)
    if latest_date:
        print(f"Found existing data. Latest post: {latest_date}")
        print(f"Fetching posts after: {latest_date}")
    else:
        print("No existing data found. Fetching all available posts.")
    
    print(f"{'-'*50}")
    print(f"Source \t\t\tPost Count")
    
    all_posts: List[Tuple[str, str, str, str, str]] = []
    
    # Reddit posts
    reddit_posts = collect_reddit_posts(ticker, latest_date)
    all_posts.extend(reddit_posts)
    print(f"Reddit  \t\t{len(reddit_posts)}")
    
    # RSS feeds
    rss_posts = collect_rss_feeds(ticker, latest_date)
    all_posts.extend(rss_posts)
    
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
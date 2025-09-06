import sqlite3
import os
import requests
import feedparser
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Tuple
from email.utils import parsedate_to_datetime
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
        logging.warning(f"Date parsing failed for '{date_str}': {e}")
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def init_database() -> str:
    """Initialize the news database and create tables if they don't exist.
    
    Returns:
        Path to the database file.
    """
    db_path = "data/news/news.db"
    
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
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

def store_article(db_path: str, ticker: str, title: str, content: str, url: str, source: str, published_at: str) -> bool:
    """Store a news article in the database.
    
    Args:
        db_path: Path to the SQLite database file.
        ticker: Stock ticker symbol.
        title: Article title.
        content: Article content/description.
        url: Article URL.
        source: News source name.
        published_at: Publication date in YYYY-MM-DD HH:MM:SS format.
        
    Returns:
        True if article was stored successfully, False otherwise.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO news 
                (ticker, title, content, url, source, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, title, content, url, source, published_at))
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to store article '{title}': {e}")
        return False

def get_latest_published_date(db_path: str, ticker: str) -> Optional[str]:
    """Get the most recent publication date for articles of a specific ticker.
    
    Args:
        db_path: Path to the SQLite database file.
        ticker: Stock ticker symbol to query.
        
    Returns:
        Latest publication date string or None if no articles found.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(published_at) FROM news 
                WHERE ticker = ?
            """, (ticker,))
            result = cursor.fetchone()
            return result[0] if result[0] else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get latest date for {ticker}: {e}")
        return None

def collect_news_for_ticker(ticker: str) -> int:
    """Collect news articles for a specific stock ticker from multiple sources.
    
    Fetches news from Yahoo Finance, RSS feeds, and NewsAPI. Only collects
    articles newer than the latest existing article in the database.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
        
    Returns:
        Number of articles successfully stored in the database.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"Starting news collection for {ticker}...")
    
    try:
        db_path = init_database()
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return 0
    
    # Check for existing data
    latest_date = get_latest_published_date(db_path, ticker)
    if latest_date:
        print(f"Found existing data. Latest article: {latest_date}")
        print(f"Fetching news after: {latest_date}")
    else:
        print("No existing data found. Fetching all available news.")
    
    print(f"{'-'*50}")
    print(f"Source  \t\tArticle Count")
    newsapi_key = os.getenv('NEWSAPI_KEY')
    
    articles: list[Tuple[str, str, str, str, str]] = []
    
    # yfinance news - fixed structure from example
    yf_count = 0
    try:
        stock = yf.Ticker(ticker)
        news = stock.news
        
        if news:
            for item in news:
                try:
                    # Extract data from nested structure like in example
                    content = item.get('content', {})
                    article_date = normalize_date(content.get('pubDate', ''))
                    
                    # Skip if article is older than latest_date
                    if latest_date and article_date <= latest_date:
                        continue
                    
                    # Get URL from clickThroughUrl or canonicalUrl
                    url = ''
                    if content.get('clickThroughUrl'):
                        url = content['clickThroughUrl'].get('url', '')
                    elif content.get('canonicalUrl'):
                        url = content['canonicalUrl'].get('url', '')
                    
                    articles.append((
                        content.get('title', 'No Title'),
                        content.get('summary', ''),
                        url,
                        f'yFinance ({ticker})',
                        article_date
                    ))
                    yf_count += 1
                except (KeyError, AttributeError, TypeError) as e:
                    logging.warning(f"Failed to process yFinance article: {e}")
                    continue
            print(f"Yahoo Finance\t\t{yf_count}")
        else:
            print(f"Yahoo Finance\t\t0")
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Yahoo Finance API error for {ticker}: {e}")
        print(f"Yahoo Finance\t\t0 (API Error)")
    
    # RSS feeds - working sources only
    feeds = [
        ('https://feeds.feedburner.com/zerohedge/feed', 'ZeroHedge'),
        ('https://feeds.a.dj.com/rss/RSSMarketsMain.xml', 'WSJ-Markets'),
        ('https://feeds.marketwatch.com/marketwatch/topstories/', 'MarketWatch'),
        ('https://feeds.bloomberg.com/markets/news.rss', 'Bloomberg'),
        ('https://feeds.marketwatch.com/marketwatch/marketpulse/', 'MarketWatch'),
        ('https://feeds.marketwatch.com/marketwatch/realtimeheadlines/', 'MarketWatch-RT'),
        ('https://www.cnbc.com/id/100003114/device/rss/rss.html', 'CNBC-Finance'),
        ('https://seekingalpha.com/market_currents.xml', 'SeekingAlpha'),
        ('https://www.investing.com/rss/news.rss', 'Investing'),
        ('https://www.fool.com/feeds/index.aspx', 'MotleyFool')
    ]
    
    for feed_url, source in feeds:
        try:
            # Add timeout and user agent for better success rate
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; NewsCollector/1.0)'}
            response = requests.get(feed_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            feed = feedparser.parse(response.content)
            if hasattr(feed, 'bozo') and feed.bozo and len(feed.entries) == 0:
                logging.warning(f"RSS feed parsing failed for {source}: {feed.bozo_exception}")
                print(f"{source}\t\t0 (Parse Error)")
                continue
            
            source_count = 0
            for entry in feed.entries:
                try:
                    article_date = normalize_date(entry.get('published', ''))
                    # Skip if article is older than latest_date
                    if latest_date and article_date <= latest_date:
                        continue
                        
                    # Filter for ticker-specific news only
                    text = f"{entry.get('title', '')} {entry.get('summary', entry.get('description', ''))}".upper()
                    if ticker.upper() in text:
                        articles.append((
                            entry.get('title', ''),
                            entry.get('summary', entry.get('description', '')),
                            entry.get('link', ''),
                            f'RSS ({source})',
                            article_date
                        ))
                        source_count += 1
                except (AttributeError, TypeError) as e:
                    logging.debug(f"Skipped RSS entry from {source}: {e}")
                    continue
            print(f"{source}\t\t{source_count}")
        except requests.RequestException as e:
            logging.debug(f"RSS feed network error for {source}: {e}")
            print(f"{source}\t\t0 (Network Error)")
            continue
        except Exception as e:
            logging.debug(f"RSS feed error for {source}: {e}")
            print(f"{source}\t\t0 (Error)")
            continue
    
    # NewsAPI - ticker-specific queries only
    if newsapi_key:
        newsapi_count = 0
        queries = [ticker, f"{ticker} stock"]
        for query in queries:
            try:
                url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&apiKey={newsapi_key}"
                if latest_date:
                    # Add from parameter to get only new articles
                    from_date = datetime.strptime(latest_date, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                    url += f"&from={from_date}"
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if data.get('status') != 'ok':
                    logging.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                    continue
                    
                for article in data.get('articles', []):
                    try:
                        article_date = normalize_date(article.get('publishedAt', ''))
                        # Skip if article is older than latest_date
                        if latest_date and article_date <= latest_date:
                            continue
                            
                        # Additional filter to ensure ticker relevance
                        text = f"{article.get('title', '')} {article.get('description', '')}".upper()
                        if ticker.upper() in text:
                            articles.append((
                                article.get('title', ''),
                                article.get('description', ''),
                                article.get('url', ''),
                                f"NewsAPI ({article.get('source', {}).get('name', 'Unknown')})",
                                article_date
                            ))
                            newsapi_count += 1
                    except (KeyError, AttributeError, TypeError) as e:
                        logging.warning(f"Failed to process NewsAPI article: {e}")
                        continue
            except requests.RequestException as e:
                logging.error(f"NewsAPI request failed for query '{query}': {e}")
                print(f"\nNewsAPI query '{query}' failed: Network error")
                continue
            except (ValueError, KeyError) as e:
                logging.error(f"NewsAPI response parsing failed for query '{query}': {e}")
                print(f"\nNewsAPI query '{query}' failed: Invalid response")
                continue
        print(f"NewsAPI \t\t{newsapi_count}")
    else:
        print("NewsAPI key not found")
    
    # Store all articles
    stored = 0
    for title, content, url, source, published_at in articles:
        if store_article(db_path, ticker, title, content, url, source, published_at):
            stored += 1
    
    print(f"{'-'*50}")
    print(f"Collected:\t{len(articles)} articles")
    print(f"Stored: \t{stored} articles")
    return stored


if __name__ == "__main__":
    collect_news_for_ticker('AAPL')
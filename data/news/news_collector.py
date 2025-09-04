import sqlite3
import os
import requests
import feedparser
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

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
    db_path = "data/news/news.db"
    # Database is in same directory as this script
    
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

def store_article(db_path, ticker, title, content, url, source, published_at):
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO news 
                (ticker, title, content, url, source, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, title, content, url, source, published_at))
            return True
        except:
            return False

def get_latest_published_date(db_path, ticker):
    """Get the latest published_at date for a ticker from database"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(published_at) FROM news 
                WHERE ticker = ?
            """, (ticker,))
            result = cursor.fetchone()
            return result[0] if result[0] else None
    except:
        return None

def collect_news_for_ticker(ticker):
    print(f"Starting news collection for {ticker}...")
    db_path = init_database()
    
    # Check for existing data
    latest_date = get_latest_published_date(db_path, ticker)
    if latest_date:
        print(f"Found existing data. Latest article: {latest_date}")
        print(f"Fetching news after: {latest_date}")
    else:
        print("No existing data found. Fetching all available news.")
    
    print(f"{'-'*50}")
    print(f"Source \t\tArticle Count")
    newsapi_key = os.getenv('NEWSAPI_KEY')
    
    articles = []
    
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
                except:
                    continue
            print(f"Yahoo Finance\t\t{yf_count}")
        else:
            print(f"Yahoo Finance\t\t0")
    except Exception as e:
        print(f"Yahoo Finance {ticker} failed: {e}")
    
    # RSS feeds - maximum sources
    feeds = [
        ('https://feeds.bloomberg.com/markets/news.rss', 'Bloomberg'),
        ('https://feeds.reuters.com/money/wealth/rss', 'Reuters '),
        ('https://www.reuters.com/business/finance/rss', 'Reuters-Finance'),
        ('https://feeds.marketwatch.com/marketwatch/marketpulse/', 'MarketWatch'),
        ('https://feeds.marketwatch.com/marketwatch/realtimeheadlines/', 'MarketWatch-RT'),
        ('https://feeds.cnbc.com/cnbc/world.rss', 'CNBC    '),
        ('https://www.cnbc.com/id/100003114/device/rss/rss.html', 'CNBC-Finance'),
        ('https://feeds.finance.yahoo.com/rss/2.0/headline', 'Yahoo-RSS'),
        ('https://feeds.feedburner.com/zerohedge/feed', 'ZeroHedge'),
        ('https://seekingalpha.com/market_currents.xml', 'SeekingAlpha'),
        ('https://www.investing.com/rss/news.rss', 'Investing'),
        ('https://feeds.benzinga.com/benzinga', 'Benzinga'),
        ('https://www.fool.com/feeds/index.aspx', 'MotleyFool'),
        ('https://feeds.barrons.com/public/rss/mdc_topstories', 'Barrons ')
    ]
    
    for feed_url, source in feeds:
        try:
            feed = feedparser.parse(feed_url)
            source_count = 0
            for entry in feed.entries:
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
            print(f"{source}\t\t{source_count}")
        except Exception as e:
            print(f"{source} failed: {e}")
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
                
                response = requests.get(url)
                if response.status_code == 200:
                    for article in response.json().get('articles', []):
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
            except Exception as e:
                print(f"\nNewsAPI query '{query}' failed: {e}")
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
    collect_news_for_ticker('GOOGL')
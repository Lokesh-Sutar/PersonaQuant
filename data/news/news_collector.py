import requests
import feedparser
import yfinance as yf
from datetime import datetime, timedelta
import sqlite3
import os
from dotenv import load_dotenv
from email.utils import parsedate_to_datetime

load_dotenv()

class NewsDB:
    def __init__(self, db_path="data/news/news.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT,
                    url TEXT UNIQUE,
                    source TEXT,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    def insert_news(self, title, content, url, source, published_at):
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO news 
                    (title, content, url, source, published_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (title, content, url, source, published_at))
                return True
            except Exception as e:
                print(f"DB insert failed: {e}")
                return False

class NewsCollector:
    def __init__(self):
        self.db = NewsDB()
        self.newsapi_key = os.getenv('NEWSAPI_KEY')
    
    def normalize_date(self, date_str):
        """Convert any date format to ISO format"""
        try:
            if 'GMT' in date_str or 'UTC' in date_str:
                # RSS format: Wed, 05 Jun 2024 11:34:00 GMT
                dt = parsedate_to_datetime(date_str)
                return dt.isoformat()
            elif 'T' in date_str:
                # ISO format: 2025-09-01T12:00:00+00:00 or 2025-08-31T12:27:47Z
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.isoformat()
            else:
                return datetime.now().isoformat()
        except:
            return datetime.now().isoformat()
    
    def collect_newsapi(self, query="finance OR stock OR market", from_date=None):
        if not self.newsapi_key:
            print("NewsAPI key not found")
            return []
        
        try:
            url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&apiKey={self.newsapi_key}"
            if from_date:
                url += f"&from={from_date.strftime('%Y-%m-%d')}"
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                raw_articles = response.json().get('articles', [])
                
                # Convert NewsAPI format to our format
                articles = []
                for article in raw_articles:
                    articles.append({
                        'title': article.get('title', ''),
                        'content': article.get('description', ''),
                        'url': article.get('url', ''),
                        'published': self.normalize_date(article.get('publishedAt', '')),
                        'source': f"NewsAPI ({article.get('source', {}).get('name', 'NewsAPI')})"
                    })
                
                print(f"NewsAPI: {len(articles)} articles")
                return articles
            else:
                print(f"NewsAPI error: {response.status_code}")
        except Exception as e:
            print(f"NewsAPI failed: {e}")
        return []
    
    def collect_rss_feeds(self):
        feeds = [
            ('https://feeds.bloomberg.com/markets/news.rss', 'Bloomberg Markets'),
            ('https://feeds.reuters.com/money/wealth/rss', 'Reuters Finance'),
            ('https://feeds.marketwatch.com/marketwatch/marketpulse/', 'MarketWatch'),
            ('https://feeds.cnbc.com/cnbc/world.rss', 'CNBC')
        ]
        
        articles = []
        for feed_url, source_name in feeds:
            try:
                print(f"Fetching {source_name}...")
                feed = feedparser.parse(feed_url)
                
                for entry in feed.entries:
                    articles.append({
                        'title': entry.title,
                        'content': entry.get('summary', entry.get('description', '')),
                        'url': entry.link,
                        'published': self.normalize_date(entry.get('published', '')),
                        'source': f"RSS ({source_name})"
                    })
                print(f"{source_name}: {len(feed.entries)} articles")
            except Exception as e:
                print(f"{source_name} failed: {e}")
                continue
        
        print(f"Total RSS articles: {len(articles)}")
        return articles
    
    def collect_yahoo_news(self, tickers=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA']):
        articles = []
        print("Fetching Yahoo Finance news...")
        
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                news = stock.news
                
                if not news:
                    print(f"Yahoo Finance {ticker}: No news available")
                    continue
                
                ticker_articles = 0
                for item in news:
                    try:
                        # Extract data from nested structure
                        content = item.get('content', {})
                        
                        # Get URL from clickThroughUrl or canonicalUrl
                        url = ''
                        if content.get('clickThroughUrl'):
                            url = content['clickThroughUrl'].get('url', '')
                        elif content.get('canonicalUrl'):
                            url = content['canonicalUrl'].get('url', '')
                        
                        articles.append({
                            'title': content.get('title', 'No Title'),
                            'content': content.get('summary', ''),
                            'url': url,
                            'published': self.normalize_date(content.get('pubDate', '')),
                            'source': f"yFinance ({ticker})"
                        })
                        ticker_articles += 1
                    except Exception as e:
                        print(f"Yahoo Finance {ticker} item error: {e}")
                        continue
                
                print(f"Yahoo Finance {ticker}: {ticker_articles} articles")
            except Exception as e:
                print(f"Yahoo Finance {ticker} failed: {e}")
                continue
        
        print(f"Total Yahoo Finance: {len(articles)} articles")
        return articles
    
    def run_daily_collection(self):
        """Collect news from all sources"""
        print("Starting news collection...")
        
        stored_counts = {'rss': 0, 'yahoo': 0, 'newsapi': 0}
        
        # Collect from RSS feeds
        rss_articles = self.collect_rss_feeds()
        for article in rss_articles:
            if self.db.insert_news(
                title=article.get('title', ''),
                content=article.get('content', ''),
                url=article.get('url', ''),
                source=article.get('source', 'Unknown'),
                published_at=article.get('published', datetime.now())
            ):
                stored_counts['rss'] += 1
        
        # Collect from Yahoo Finance
        yahoo_articles = self.collect_yahoo_news()
        for article in yahoo_articles:
            if self.db.insert_news(
                title=article.get('title', ''),
                content=article.get('content', ''),
                url=article.get('url', ''),
                source=article.get('source', 'Unknown'),
                published_at=article.get('published', datetime.now())
            ):
                stored_counts['yahoo'] += 1
        
        # Collect from NewsAPI
        newsapi_articles = self.collect_newsapi()
        for article in newsapi_articles:
            if self.db.insert_news(
                title=article.get('title', ''),
                content=article.get('content', ''),
                url=article.get('url', ''),
                source=article.get('source', 'NewsAPI'),
                published_at=article.get('published', datetime.now())
            ):
                stored_counts['newsapi'] += 1
        
        total_stored = sum(stored_counts.values())
        total_collected = len(rss_articles) + len(yahoo_articles) + len(newsapi_articles)
        
        print(f"Collected: {total_collected} articles")
        print(f"Stored: RSS={stored_counts['rss']}, Yahoo={stored_counts['yahoo']}, NewsAPI={stored_counts['newsapi']}")
        print(f"Total stored: {total_stored} articles")
        print(f"Duplicates skipped: {total_collected - total_stored}")
        
        return total_stored
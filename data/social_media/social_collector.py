import sqlite3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class SocialDB:
    def __init__(self, db_path="data/social_media/social.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS social_posts (
                    id INTEGER PRIMARY KEY,
                    platform TEXT,
                    content TEXT,
                    author TEXT,
                    url TEXT UNIQUE,
                    engagement INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    def insert_post(self, platform, content, author, url, engagement):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO social_posts 
                (platform, content, author, url, engagement)
                VALUES (?, ?, ?, ?, ?)
            """, (platform, content, author, url, engagement))
    
    def get_latest_date(self):
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("""
                SELECT MAX(created_at) FROM social_posts
            """).fetchone()
            return result[0] if result[0] else None

class SocialCollector:
    def __init__(self):
        self.db = SocialDB()
        self.reddit_client_id = os.getenv('REDDIT_CLIENT_ID')
        self.reddit_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.reddit_redirect = os.getenv('REDDIT_REDIRECT_URI')
    
    def collect_reddit_posts(self, subreddits=['investing', 'stocks'], limit=100):
        try:
            import praw
            reddit = praw.Reddit(
                client_id=self.reddit_client_id,
                client_secret=self.reddit_secret,
                redirect_uri=self.reddit_redirect,
                user_agent=os.getenv('REDDIT_USER_AGENT')
            )
            
            posts = []
            for sub in subreddits:
                subreddit = reddit.subreddit(sub)
                for post in subreddit.hot(limit=limit):
                    posts.append({
                        'platform': 'reddit',
                        'content': f"{post.title}\n{post.selftext}",
                        'author': str(post.author),
                        'url': f"https://reddit.com{post.permalink}",
                        'engagement': post.score
                    })
            return posts
        except:
            return []
    
    def run_initial_collection(self):
        """Collect historical data for initial setup"""
        all_posts = []
        all_posts.extend(self.collect_reddit_posts(limit=300))
        
        for post in all_posts:
            self.db.insert_post(
                platform=post['platform'],
                content=post['content'],
                author=post['author'],
                url=post['url'],
                engagement=post['engagement']
            )
        
        print(f"Initial collection: {len(all_posts)} social posts")
        return len(all_posts)
    
    def run_daily_collection(self):
        """Collect only new data since last update"""
        latest_date = self.db.get_latest_date()
        
        if not latest_date:
            print("No existing social data found. Running initial collection...")
            return self.run_initial_collection()
        
        print(f"Collecting social posts since: {latest_date}")
        
        all_posts = []
        all_posts.extend(self.collect_reddit_posts())
        
        for post in all_posts:
            self.db.insert_post(
                platform=post['platform'],
                content=post['content'],
                author=post['author'],
                url=post['url'],
                engagement=post['engagement']
            )
        
        print(f"Collected {len(all_posts)} social media posts")
        return len(all_posts)
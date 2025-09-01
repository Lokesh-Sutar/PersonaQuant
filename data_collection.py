from data.news.news_collector import NewsCollector
from data.social_media.social_collector import SocialCollector
import schedule
import time

def run_daily_data_collection():
    print("Starting daily data collection...")
    
    try:
        # Collect news data
        news_collector = NewsCollector()
        news_count = news_collector.run_daily_collection()
        
        # Collect social media data
        social_collector = SocialCollector()
        social_count = social_collector.run_daily_collection()
        
        print(f"Data collection completed: {news_count} news, {social_count} social posts")
    except Exception as e:
        print(f"Collection failed: {e}")

if __name__ == "__main__":
    # Run immediately
    run_daily_data_collection()
    
    # Schedule daily runs at 9 AM
    schedule.every().day.at("09:00").do(run_daily_data_collection)
    
    print("Data collection scheduler started. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)
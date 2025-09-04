import sqlite3
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data.news.news_collector import collect_news_for_ticker
from data.social_media.social_collector import collect_social_for_ticker

analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    """Analyze sentiment of text using VADER"""
    if not text:
        return 0.0
    
    scores = analyzer.polarity_scores(text)
    return scores['compound']  # Returns -1 to 1

def get_news_sentiment(ticker, days=7):
    """Get sentiment from news database with top positive/negative articles"""
    db_path = "data/news/news.db"
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get news from last N days
            date_limit = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute("""
                SELECT title, content, url, published_at FROM news 
                WHERE ticker = ? AND published_at >= ?
            """, (ticker, date_limit))
            
            articles = cursor.fetchall()
            
            if not articles:
                return {"count": 0, "sentiment": 0.0, "top_positive": [], "top_negative": []}
            
            article_sentiments = []
            for title, content, url, published_at in articles:
                text = f"{title}; {content or ''}"
                sentiment = analyze_sentiment(text)
                article_sentiments.append({
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "sentiment": sentiment
                })
            
            # Sort by sentiment
            sorted_articles = sorted(article_sentiments, key=lambda x: x["sentiment"], reverse=True)
            
            # Get top 10 positive and negative
            top_positive = [{
                "title": art["title"],
                "url": art["url"],
                "published_at": art["published_at"],
                "sentiment_score": round(art["sentiment"], 3)
            } for art in sorted_articles[:10] if art["sentiment"] > 0]
            
            top_negative = [{
                "title": art["title"],
                "url": art["url"],
                "published_at": art["published_at"],
                "sentiment_score": round(art["sentiment"], 3)
            } for art in sorted_articles[-10:] if art["sentiment"] < 0]
            
            avg_sentiment = sum([art["sentiment"] for art in article_sentiments]) / len(article_sentiments)
            
            return {
                "count": len(articles),
                "sentiment": round(avg_sentiment, 3),
                "top_positive": top_positive,
                "top_negative": list(reversed(top_negative))  # Most negative first
            }
    
    except Exception as e:
        return {"count": 0, "sentiment": 0.0, "top_positive": [], "top_negative": [], "error": str(e)}

def get_social_sentiment(ticker, days=7):
    """Get sentiment from social media database"""
    db_path = "data/social_media/social.db"
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get posts from last N days
            date_limit = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute("""
                SELECT title, content FROM social_posts 
                WHERE ticker = ? AND published_at >= ?
            """, (ticker, date_limit))
            
            posts = cursor.fetchall()
            
            if not posts:
                return {"count": 0, "sentiment": 0.0}
            
            sentiments = []
            for title, content in posts:
                text = f"{title}; {content or ''}"
                sentiment = analyze_sentiment(text)
                sentiments.append(sentiment)
            
            avg_sentiment = sum(sentiments) / len(sentiments)
            
            return {
                "count": len(posts),
                "sentiment": round(avg_sentiment, 3)
            }
    
    except Exception as e:
        return {"count": 0, "sentiment": 0.0, "error": str(e)}

def get_sentiment_score(ticker, days=7):
    """Main function to get sentiment score for a ticker"""
    
    # Generating Latest Data
    collect_news_for_ticker(ticker)
    print('='*50)
    collect_social_for_ticker(ticker)
    print('='*50)

    # Get news sentiment
    news_data = get_news_sentiment(ticker, days)
    
    # Get social media sentiment
    social_data = get_social_sentiment(ticker, days)
    
    # Calculate overall sentiment
    total_count = news_data["count"] + social_data["count"]
    
    if total_count == 0:
        overall_sentiment = 0.0
    else:
        # Weighted average based on count
        overall_sentiment = (
            (news_data["sentiment"] * news_data["count"]) + 
            (social_data["sentiment"] * social_data["count"])
        ) / total_count
    
    # Sentiment interpretation
    if overall_sentiment > 0.1:
        sentiment_label = "Positive"
    elif overall_sentiment < -0.1:
        sentiment_label = "Negative"
    else:
        sentiment_label = "Neutral"
    
    result = {
        "ticker": ticker,
        "analysis_period_days": days,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "overall_sentiment": {
            "score": round(overall_sentiment, 3),
            "label": sentiment_label
        },
        "news_sentiment": {
            "score": news_data["sentiment"],
            "article_count": news_data["count"]
        },
        "social_sentiment": {
            "score": social_data["sentiment"],
            "post_count": social_data["count"]
        },
        "total_data_points": total_count,
        "top_positive_news": news_data.get("top_positive", []),
        "top_negative_news": news_data.get("top_negative", [])
    }
    
    return json.dumps(result, indent=4)


if __name__ == "__main__":
    result = get_sentiment_score('GOOGL', days=7)
    print(result)
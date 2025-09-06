import sqlite3
import json
from typing import Dict, List, Any, Optional
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from data.news.news_collector import collect_news_for_ticker
from data.social_media.social_collector import collect_social_for_ticker

analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text: Optional[str]) -> float:
    """Analyze sentiment of text using VADER sentiment analyzer.
    
    Args:
        text: Input text to analyze. Can be None or empty.
        
    Returns:
        Sentiment score between -1 (most negative) and 1 (most positive).
        Returns 0.0 for empty/None text.
    """
    if not text:
        return 0.0
    
    scores = analyzer.polarity_scores(text)
    return scores['compound']

def get_news_sentiment(ticker: str, days: int = 7) -> Dict[str, Any]:
    """Retrieve and analyze sentiment from news articles for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
        days: Number of days to look back for news articles.
        
    Returns:
        Dictionary containing:
        - count: Number of articles analyzed
        - sentiment: Average sentiment score (-1 to 1)
        - top_positive: List of most positive articles
        - top_negative: List of most negative articles
        - error: Error message if database operation fails
    """
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

def get_social_sentiment(ticker: str, days: int = 7) -> Dict[str, Any]:
    """Retrieve and analyze sentiment from social media posts for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
        days: Number of days to look back for social media posts.
        
    Returns:
        Dictionary containing:
        - count: Number of posts analyzed
        - sentiment: Average sentiment score (-1 to 1)
        - error: Error message if database operation fails
    """
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

def get_sentiment_score(ticker: str, days: int = 7) -> str:
    """Generate comprehensive sentiment analysis for a stock ticker.
    
    Collects fresh news and social media data, then analyzes sentiment
    from both sources to provide an overall sentiment assessment.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL').
        days: Number of days to analyze historical data.
        
    Returns:
        JSON string containing complete sentiment analysis including:
        - Overall sentiment score and label
        - News sentiment breakdown
        - Social media sentiment breakdown
        - Top positive/negative news articles
        - Total data points analyzed
    """
    
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
    result = get_sentiment_score('AAPL', days=7)
    print(result)
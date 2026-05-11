import os, json, time
from datetime import datetime, timezone
import feedparser

INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "BBC": "https://feeds.bbci.co.uk/news/rss.xml",
    "CNN": "http://rss.cnn.com/rss/edition.rss",
    "AlJazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "Reuters": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best"
}

def pull_once(tick):
    rows = []

    for source, url in FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:10]:
                rows.append({
                    "source": source,
                    "title": entry.get("title", ""),
                    "url": entry.get("link", ""),
                    "ts": datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            print(f"Feed failed: {source} - {e}")

    path = os.path.join(INCOMING, f"batch_{tick}.json")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    print(f"Wrote {len(rows)} rows to {path}")

if __name__ == "__main__":
    tick = 0
    while True:
        pull_once(tick)
        tick += 1
        time.sleep(60)

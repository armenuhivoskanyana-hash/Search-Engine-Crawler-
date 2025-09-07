# Web Crawler for Search Engine

A high-performance, asynchronous web crawler designed for building search engine indexes. Features include robots.txt compliance, duplicate detection, persistent queues, and comprehensive content extraction.

## Features

- **Asynchronous crawling** with configurable concurrency
- **Robots.txt compliance** to respect website policies
- **Persistent URL queue** using SQLite for crash recovery
- **Duplicate detection** using content hashing
- **Priority-based crawling** with depth limiting
- **Comprehensive content extraction** (text, links, metadata)
- **Rate limiting** with random delays between requests
- **Statistics tracking** and progress monitoring
- **Database storage** for crawled content
- **JSON export** for processed pages

## Installation

### Requirements

Create a `requirements.txt` file:

```
aiohttp>=3.8.0
aiofiles>=0.8.0
beautifulsoup4>=4.11.0
asyncio>=3.4.3
```

### Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
import asyncio
from crawler import WebCrawler

async def main():
    # Initialize crawler
    crawler = WebCrawler(
        max_concurrent=10,
        max_depth=3,
        delay_range=(1.0, 3.0),
        output_dir="crawled_data"
    )
    
    # Add seed URLs
    seed_urls = [
        "https://example.com",
        "https://another-site.com",
    ]
    crawler.add_seed_urls(seed_urls)
    
    # Start crawling
    await crawler.crawl()
    
    # Get statistics
    stats = crawler.get_statistics()
    print(f"Crawled {stats['pages_crawled']} pages")

if __name__ == "__main__":
    asyncio.run(main())
```

### Command Line Usage

```bash
python crawler.py --seeds https://example.com https://another-site.com \
                  --max-depth 2 \
                  --max-concurrent 5 \
                  --output-dir ./data \
                  --delay 1.0 2.0
```

### Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `max_concurrent` | Maximum concurrent HTTP requests | 10 |
| `max_depth` | Maximum crawling depth from seed URLs | 3 |
| `delay_range` | Random delay range between requests (seconds) | (1.0, 3.0) |
| `user_agent` | User agent string for HTTP requests | "SearchBot/1.0" |
| `output_dir` | Directory for storing crawled data | "crawled_data" |
| `db_path` | SQLite database path | "crawler.db" |

## Architecture

### Core Components

1. **WebCrawler**: Main orchestrator class
2. **URLQueue**: Priority-based URL queue with SQLite persistence
3. **RobotsTxtChecker**: Handles robots.txt compliance
4. **ContentProcessor**: Extracts and processes HTML content
5. **CrawledPage**: Data structure for crawled page information

### Data Flow

1. Seeds are added to the URL queue with high priority
2. Worker coroutines fetch URLs concurrently
3. Each page is processed for content and links
4. Extracted links are added back to the queue
5. Processed pages are saved to database and files

### Database Schema

#### URL Queue Table
```sql
CREATE TABLE url_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    priority INTEGER DEFAULT 1,
    depth INTEGER DEFAULT 0,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    crawled BOOLEAN DEFAULT FALSE
);
```

#### Crawled Pages Table
```sql
CREATE TABLE crawled_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    title TEXT,
    content TEXT,
    meta_description TEXT,
    content_hash TEXT,
    status_code INTEGER,
    content_type TEXT,
    page_size INTEGER,
    load_time REAL,
    outbound_links TEXT,  -- JSON array
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Output Format

### JSON Files
Each crawled page is saved as a JSON file with the following structure:

```json
{
  "url": "https://example.com/page",
  "title": "Page Title",
  "content": "Extracted text content...",
  "meta_description": "Page description",
  "status_code": 200,
  "content_type": "text/html",
  "content_hash": "md5_hash_of_content",
  "outbound_links": ["https://link1.com", "https://link2.com"],
  "page_size": 15420,
  "load_time": 1.23,
  "last_crawled": "2024-01-15T10:30:45.123456"
}
```

## Best Practices

### Respectful Crawling
- Honors robots.txt files
- Implements delays between requests
- Uses appropriate user agent
- Limits concurrent requests per domain

### Performance Optimization
- Async I/O for maximum throughput
- Persistent queue for crash recovery
- Content deduplication using hashes
- Efficient database indexing

### Error Handling
- Timeout protection for requests
- Graceful handling of HTTP errors
- Logging of failed crawls
- Retry mechanisms where appropriate

## Monitoring and Statistics

The crawler provides real-time statistics:

```python
stats = crawler.get_statistics()
print(f"Pages crawled: {stats['pages_crawled']}")
print(f"Pages failed: {stats['pages_failed']}")
print(f"Queue size: {stats['queue_size']}")
print(f"Domains crawled: {stats['domains_crawled']}")
print(f"Total data size: {stats['total_size']} bytes")
```

## Integration with Search Engine

This crawler is designed to integrate with larger search engine components:

1. **Index Builder**: Process crawled content for search indexing
2. **Link Graph**: Build web graph from extracted links
3. **Content Analysis**: Analyze content for ranking signals
4. **Freshness Monitoring**: Track content changes over time

## Extending the Crawler

### Adding Custom Content Processors
```python
class CustomContentProcessor(ContentProcessor):
    @staticmethod
    def extract_custom_data(html: str) -> dict:
        # Custom extraction logic
        return {"custom_field": "extracted_data"}
```

### Adding URL Filters
```python
def custom_url_filter(url: str) -> bool:
    # Return True if URL should be crawled
    return "unwanted-path" not in url

# Integrate into crawler logic
```

## Troubleshooting

### Common Issues

1. **High memory usage**: Reduce `max_concurrent` parameter
2. **Slow crawling**: Increase concurrency or reduce delays
3. **Blocked by robots.txt**: Check robots.txt compliance
4. **Database locks**: Ensure proper connection handling
5. **Timeout errors**: Increase timeout values

### Logging

The crawler logs to both file (`crawler.log`) and console:
- INFO: Normal operations and statistics
- WARNING: Non-critical issues (HTTP errors, robots.txt blocks)
- ERROR: Critical errors that prevent crawling

## Performance Tuning

### Recommended Settings by Scale

| Scale | Concurrent | Depth | Delay | Notes |
|-------|------------|-------|-------|-------|
| Small (< 10K pages) | 5 | 2 | 1-2s | Conservative settings |
| Medium (10K-100K) | 15 | 3 | 0.5-1.5s | Balanced approach |
| Large (> 100K) | 25+ | 4+ | 0.2-1s | High performance |

### Hardware Recommendations
- **CPU**: Multi-core for async processing
- **RAM**: 4GB+ for large crawls
- **Storage**: SSD for database performance
- **Network**: Stable, high-bandwidth connection

## License

This crawler is designed for educational and research purposes. Ensure compliance with website terms of service and applicable laws when crawling.

---

For questions or contributions, please refer to the project repository.
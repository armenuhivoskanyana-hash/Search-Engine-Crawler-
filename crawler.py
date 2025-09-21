import asyncio
import aiohttp
import aiofiles
import json
import logging
import time
import urllib.robotparser
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Set, List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import re
from bs4 import BeautifulSoup
import sqlite3
from contextlib import asynccontextmanager
import random
from config import MONGODB_CONNECTION
import pymongo
import ssl

@dataclass
class CrawledPage:
    """Represents a crawled web page with metadata"""
    url: str
    content: str
    title: str
    meta_description: str
    headers: Dict[str, str]
    status_code: int
    content_type: str
    last_crawled: str
    content_hash: str
    outbound_links: List[str]
    page_size: int
    load_time: float

class MongoDBIndexer:
    """Handles MongoDB indexing for search engine"""
    
    def __init__(self, connection_string: str):
        """Initialize MongoDB connection"""
        try:
            self.client = pymongo.MongoClient(
                connection_string,
                ssl_cert_reqs=ssl.CERT_NONE  # For Mac SSL issues
            )
            self.db = self.client['search_engine']
            self.collection = self.db['pages']
            
            # Test connection
            self.client.admin.command('ping')
            
            # Setup indexes for better search performance
            self._setup_indexes()
            
            logging.info("✓ MongoDB connection successful!")
            
        except Exception as e:
            logging.error(f"✗ MongoDB connection failed: {e}")
            raise
    
    def _setup_indexes(self):
        """Create indexes for efficient searching"""
        try:
            # Text index for full-text search
            self.collection.create_index([
                ("title", "text"),
                ("content", "text"),
                ("meta_description", "text")
            ])
            
            # URL index for uniqueness and quick lookups
            self.collection.create_index("url", unique=True)
            
            # Domain index for filtering by site
            self.collection.create_index("domain")
            
            # Date index for freshness sorting
            self.collection.create_index("crawled_at")
            
            # Content hash for deduplication
            self.collection.create_index("content_hash")
            
            logging.info("✓ MongoDB indexes created successfully!")
            
        except pymongo.errors.DuplicateKeyError:
            logging.info("MongoDB indexes already exist")
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        return urlparse(url).netloc
    
    def generate_page_id(self, url: str) -> str:
        """Generate unique ID for page"""
        return hashlib.md5(url.encode()).hexdigest()
    
    async def index_page(self, page: CrawledPage) -> bool:
        """Index a single crawled page to MongoDB"""
        try:
            # Prepare document for MongoDB
            page_doc = {
                '_id': self.generate_page_id(page.url),
                'url': page.url,
                'title': page.title,
                'content': page.content,
                'meta_description': page.meta_description,
                'keywords': self._extract_keywords(page.content),
                'outbound_links': page.outbound_links,
                'domain': self.extract_domain(page.url),
                'status_code': page.status_code,
                'content_type': page.content_type,
                'content_hash': page.content_hash,
                'content_length': page.page_size,
                'word_count': len(page.content.split()),
                'load_time': page.load_time,
                'crawled_at': datetime.now(timezone.utc),
                'last_crawled': page.last_crawled,
                'indexed': True
            }
            
            # Insert or update (upsert)
            result = self.collection.replace_one(
                {'_id': page_doc['_id']}, 
                page_doc, 
                upsert=True
            )
            
            if result.upserted_id:
                logging.info(f"✓ Indexed new page: {page.url}")
            else:
                logging.info(f"✓ Updated existing page: {page.url}")
                
            return True
            
        except Exception as e:
            logging.error(f"✗ Error indexing {page.url}: {e}")
            return False
    
    def _extract_keywords(self, content: str) -> List[str]:
        """Extract simple keywords from content"""
        # Simple keyword extraction (you can make this more sophisticated)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())
        # Get most common words (excluding common stop words)
        stop_words = {'the', 'and', 'are', 'for', 'not', 'but', 'this', 'that', 'with', 'from'}
        keywords = [word for word in words if word not in stop_words]
        # Return top 20 most frequent keywords
        from collections import Counter
        return [word for word, count in Counter(keywords).most_common(20)]
    
    def search_pages(self, query: str, limit: int = 10) -> List[Dict]:
        """Search indexed pages"""
        try:
            results = self.collection.find(
                {"$text": {"$search": query}},
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)
            
            return list(results)
            
        except Exception as e:
            logging.error(f"Search error: {e}")
            return []
    
    def get_indexed_count(self) -> int:
        """Get total number of indexed pages"""
        return self.collection.count_documents({})
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed")

class RobotsTxtChecker:
    """Handles robots.txt compliance"""
    
    def __init__(self):
        self._robots_cache = {}
    
    async def can_crawl(self, session: aiohttp.ClientSession, url: str, user_agent: str = "*") -> bool:
        """Check if URL can be crawled according to robots.txt"""
        try:
            parsed_url = urlparse(url)
            robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
            
            if robots_url not in self._robots_cache:
                try:
                    async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            robots_content = await response.text()
                            rp = urllib.robotparser.RobotFileParser()
                            rp.set_url(robots_url)
                            rp.feed(robots_content)
                            self._robots_cache[robots_url] = rp
                        else:
                            # If no robots.txt, assume crawling is allowed
                            self._robots_cache[robots_url] = None
                except:
                    self._robots_cache[robots_url] = None
            
            robots_parser = self._robots_cache[robots_url]
            if robots_parser is None:
                return True
            
            return robots_parser.can_fetch(user_agent, url)
        except:
            return True  # Default to allowing crawling if check fails

class URLQueue:
    """Priority-based URL queue with deduplication"""
    
    def __init__(self, db_path: str = "crawler_queue.db"):
        self.db_path = db_path
        self._setup_database()
        self._seen_urls = set()
        self._load_seen_urls()
    
    def _setup_database(self):
        """Initialize SQLite database for persistent queue"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS url_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                priority INTEGER DEFAULT 1,
                depth INTEGER DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                crawled BOOLEAN DEFAULT FALSE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_priority_crawled ON url_queue(priority DESC, crawled)")
        conn.commit()
        conn.close()
    
    def _load_seen_urls(self):
        """Load previously seen URLs to avoid duplicates"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT url FROM url_queue")
        self._seen_urls = {row[0] for row in cursor.fetchall()}
        conn.close()
    
    def add_url(self, url: str, priority: int = 1, depth: int = 0) -> bool:
        """Add URL to queue if not already seen"""
        if url in self._seen_urls:
            return False
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO url_queue (url, priority, depth) VALUES (?, ?, ?)",
                (url, priority, depth)
            )
            conn.commit()
            conn.close()
            self._seen_urls.add(url)
            return True
        except sqlite3.IntegrityError:
            return False
    
    def get_next_url(self) -> Optional[Tuple[str, int]]:
        """Get next URL to crawl with highest priority"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT url, depth FROM url_queue WHERE crawled = FALSE ORDER BY priority DESC, id ASC LIMIT 1"
        )
        result = cursor.fetchone()
        if result:
            url, depth = result
            conn.execute("UPDATE url_queue SET crawled = TRUE WHERE url = ?", (url,))
            conn.commit()
        conn.close()
        return result
    
    def get_queue_size(self) -> int:
        """Get number of uncrawled URLs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM url_queue WHERE crawled = FALSE")
        count = cursor.fetchone()[0]
        conn.close()
        return count

class ContentProcessor:
    """Processes and extracts content from HTML"""
    
    @staticmethod
    def extract_text_content(html: str) -> str:
        """Extract clean text content from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "noscript"]):
                script.decompose()
            
            # Get text and clean it up
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        except:
            return ""
    
    @staticmethod
    def extract_links(html: str, base_url: str) -> List[str]:
        """Extract all links from HTML"""
        links = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(base_url, href)
                
                # Basic URL validation and filtering
                parsed = urlparse(absolute_url)
                if parsed.scheme in ('http', 'https') and parsed.netloc:
                    # Remove fragments and normalize
                    clean_url = urlunparse((
                        parsed.scheme, parsed.netloc, parsed.path,
                        parsed.params, parsed.query, ''
                    ))
                    links.append(clean_url)
        except:
            pass
        
        return list(set(links))  # Remove duplicates
    
    @staticmethod
    def extract_metadata(html: str) -> Dict[str, str]:
        """Extract title and meta description"""
        metadata = {'title': '', 'meta_description': ''}
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.get_text().strip()
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                metadata['meta_description'] = meta_desc['content'].strip()
            
        except:
            pass
        
        return metadata

class WebCrawler:
    """Main web crawler class with MongoDB integration"""
    
    def __init__(self, 
                 max_concurrent: int = 10,
                 max_depth: int = 3,
                 delay_range: Tuple[float, float] = (1.0, 3.0),
                 user_agent: str = "SearchBot/1.0",
                 output_dir: str = "crawled_data",
                 db_path: str = "crawler.db"):
        
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.delay_range = delay_range
        self.user_agent = user_agent
        self.output_dir = output_dir
        self.db_path = db_path
        
        self.url_queue = URLQueue()
        self.robots_checker = RobotsTxtChecker()
        self.content_processor = ContentProcessor()
        
        # Initialize MongoDB indexer
        self.mongodb_indexer = MongoDBIndexer(MONGODB_CONNECTION)
        
        # Statistics
        self.stats = {
            'pages_crawled': 0,
            'pages_failed': 0,
            'pages_indexed': 0,
            'total_size': 0,
            'start_time': None,
            'domains_crawled': set()
        }
        
        # Setup logging
        self._setup_logging()
        self._setup_database()
    
    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('crawler.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _setup_database(self):
        """Setup database for storing crawled pages"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crawled_pages (
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
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON crawled_pages(url)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crawled_at ON crawled_pages(crawled_at)")
        conn.commit()
        conn.close()
    
    async def _fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[CrawledPage]:
        """Fetch and process a single page"""
        start_time = time.time()
        
        try:
            # Check robots.txt
            if not await self.robots_checker.can_crawl(session, url, self.user_agent):
                self.logger.info(f"Blocked by robots.txt: {url}")
                return None
            
            # Add random delay to be respectful
            await asyncio.sleep(random.uniform(*self.delay_range))
            
            headers = {
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    self.logger.warning(f"HTTP {response.status} for {url}")
                    return None
                
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' not in content_type:
                    self.logger.info(f"Skipping non-HTML content: {url}")
                    return None
                
                html_content = await response.text()
                load_time = time.time() - start_time
                
                # Process content
                text_content = self.content_processor.extract_text_content(html_content)
                metadata = self.content_processor.extract_metadata(html_content)
                outbound_links = self.content_processor.extract_links(html_content, url)
                
                # Create content hash
                content_hash = hashlib.md5(text_content.encode()).hexdigest()
                
                page = CrawledPage(
                    url=url,
                    content=text_content,
                    title=metadata['title'],
                    meta_description=metadata['meta_description'],
                    headers=dict(response.headers),
                    status_code=response.status,
                    content_type=content_type,
                    last_crawled=datetime.now().isoformat(),
                    content_hash=content_hash,
                    outbound_links=outbound_links,
                    page_size=len(html_content),
                    load_time=load_time
                )
                
                self.logger.info(f"Successfully crawled: {url}")
                return page
                
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout for {url}")
        except Exception as e:
            self.logger.error(f"Error crawling {url}: {str(e)}")
        
        return None
    
    async def _save_page(self, page: CrawledPage):
        """Save crawled page to database, file system, AND MongoDB"""
        # Save to SQLite database (original functionality)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO crawled_pages 
                (url, title, content, meta_description, content_hash, status_code, 
                 content_type, page_size, load_time, outbound_links) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                page.url, page.title, page.content, page.meta_description,
                page.content_hash, page.status_code, page.content_type,
                page.page_size, page.load_time, json.dumps(page.outbound_links)
            ))
            conn.commit()
        finally:
            conn.close()
        
        # Save to JSON file (original functionality)
        import os
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create filename from URL hash
        url_hash = hashlib.md5(page.url.encode()).hexdigest()
        filename = f"{self.output_dir}/page_{url_hash}.json"
        
        async with aiofiles.open(filename, 'w') as f:
            await f.write(json.dumps(asdict(page), indent=2, ensure_ascii=False))
        
        # NEW: Index to MongoDB for search functionality
        if await self.mongodb_indexer.index_page(page):
            self.stats['pages_indexed'] += 1
    
    async def _crawl_worker(self, session: aiohttp.ClientSession):
        """Worker coroutine for crawling pages"""
        while True:
            url_data = self.url_queue.get_next_url()
            if not url_data:
                break
            
            url, depth = url_data
            
            if depth > self.max_depth:
                continue
            
            page = await self._fetch_page(session, url)
            
            if page:
                await self._save_page(page)
                
                # Add outbound links to queue
                domain = urlparse(url).netloc
                for link in page.outbound_links[:50]:  # Limit links per page
                    link_domain = urlparse(link).netloc
                    # Prioritize same domain links
                    priority = 2 if link_domain == domain else 1
                    self.url_queue.add_url(link, priority, depth + 1)
                
                self.stats['pages_crawled'] += 1
                self.stats['total_size'] += page.page_size
                self.stats['domains_crawled'].add(domain)
            else:
                self.stats['pages_failed'] += 1
            
            # Log progress
            if self.stats['pages_crawled'] % 10 == 0 and self.stats['pages_crawled'] > 0:  # More frequent logging
                self._log_stats()
    
    def _log_stats(self):
        """Log crawler statistics"""
        elapsed = time.time() - self.stats['start_time']
        pages_per_sec = self.stats['pages_crawled'] / elapsed if elapsed > 0 else 0
        queue_size = self.url_queue.get_queue_size()
        mongodb_count = self.mongodb_indexer.get_indexed_count()
        
        self.logger.info(
            f"Stats: {self.stats['pages_crawled']} crawled, "
            f"{self.stats['pages_indexed']} indexed to MongoDB, "
            f"{self.stats['pages_failed']} failed, "
            f"{queue_size} queued, "
            f"{pages_per_sec:.2f} pages/sec, "
            f"{len(self.stats['domains_crawled'])} domains, "
            f"{mongodb_count} total in search index"
        )
    
    def add_seed_urls(self, urls: List[str], priority: int = 5):
        """Add seed URLs to start crawling"""
        for url in urls:
            self.url_queue.add_url(url, priority, 0)
        self.logger.info(f"Added {len(urls)} seed URLs")
    
    async def crawl(self):
        """Start the crawling process"""
        self.stats['start_time'] = time.time()
        self.logger.info("Starting web crawler with MongoDB indexing...")
        
        # Create SSL context that uses system certificates
        ssl_context = ssl.create_default_context()
        
        connector = aiohttp.TCPConnector(
            limit=100, 
            limit_per_host=10,
            ssl=ssl_context  # Use system SSL certificates
        )
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create worker tasks
            workers = [
                asyncio.create_task(self._crawl_worker(session))
                for _ in range(self.max_concurrent)
            ]
            
            # Wait for all workers to complete
            await asyncio.gather(*workers)
        
        self._log_stats()
        self.logger.info("Crawling completed!")
        
        # Close MongoDB connection
        self.mongodb_indexer.close_connection()
    
    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """Search indexed pages"""
        return self.mongodb_indexer.search_pages(query, limit)
    
    def get_statistics(self) -> Dict:
        """Get crawler statistics"""
        stats = self.stats.copy()
        stats['queue_size'] = self.url_queue.get_queue_size()
        stats['domains_crawled'] = len(self.stats['domains_crawled'])
        stats['mongodb_indexed_count'] = self.mongodb_indexer.get_indexed_count()
        if self.stats['start_time']:
            stats['elapsed_time'] = time.time() - self.stats['start_time']
        return stats

# CLI interface
async def main():
    """Main function for running the crawler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Web Crawler for Search Engine with MongoDB')
    parser.add_argument('--seeds', nargs='+', required=True, help='Seed URLs to start crawling')
    parser.add_argument('--max-depth', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--max-concurrent', type=int, default=10, help='Maximum concurrent requests')
    parser.add_argument('--output-dir', default='crawled_data', help='Output directory')
    parser.add_argument('--delay', type=float, nargs=2, default=[1.0, 3.0], help='Delay range between requests')
    parser.add_argument('--search', type=str, help='Search query to test after crawling')
    
    args = parser.parse_args()
    
    # Initialize crawler
    crawler = WebCrawler(
        max_concurrent=args.max_concurrent,
        max_depth=args.max_depth,
        delay_range=tuple(args.delay),
        output_dir=args.output_dir
    )
    
    # Add seed URLs
    crawler.add_seed_urls(args.seeds)
    
    # Start crawling
    try:
        await crawler.crawl()
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
    finally:
        print("\nFinal Statistics:")
        stats = crawler.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Test search if query provided
        if args.search:
            print(f"\nSearch results for '{args.search}':")
            results = crawler.search(args.search)
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['title']}")
                print(f"   URL: {result['url']}")
                print(f"   Score: {result.get('score', 'N/A')}")
                print()

if __name__ == "__main__":
    asyncio.run(main())
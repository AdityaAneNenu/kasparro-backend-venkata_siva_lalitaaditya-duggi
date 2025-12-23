"""RSS feed data extractor."""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, Generator, Optional

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import get_settings
from core.exceptions import ExtractionError
from core.models import RawRSSData, SourceType, UnifiedData
from ingestion.base import BaseExtractor
from services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
settings = get_settings()


class RSSExtractor(BaseExtractor):
    """Extractor for RSS feed data source."""

    source_type = SourceType.RSS

    def __init__(
        self,
        db: Session,
        feed_url: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ):
        super().__init__(db, rate_limiter, **kwargs)
        self.feed_url = feed_url or settings.RSS_SOURCE_URL

    def _strip_html(self, text: str) -> str:
        """Remove HTML tags from text."""
        if not text:
            return ""
        # Unescape HTML entities
        text = unescape(text)
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Clean up whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse RSS date formats."""
        if not date_str:
            return None

        try:
            # RFC 2822 format (common in RSS)
            return parsedate_to_datetime(date_str)
        except (ValueError, TypeError):
            pass

        # Try ISO format
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

        # Try common formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return None

    def _get_element_text(
        self, element: ET.Element, tag: str, namespace: str = ""
    ) -> Optional[str]:
        """Get text content of a child element."""
        if namespace:
            child = element.find(f"{{{namespace}}}{tag}")
        else:
            child = element.find(tag)

        return child.text.strip() if child is not None and child.text else None

    def _parse_item(self, item: ET.Element, namespaces: Dict[str, str]) -> Dict[str, Any]:
        """Parse an RSS item element."""
        # Get basic fields
        title = self._get_element_text(item, "title")
        link = self._get_element_text(item, "link")
        description = self._get_element_text(item, "description")
        pub_date = self._get_element_text(item, "pubDate")
        guid = self._get_element_text(item, "guid")
        author = self._get_element_text(item, "author")

        # Try to get content:encoded for full content
        content = None
        for ns_prefix, ns_uri in namespaces.items():
            if "content" in ns_prefix.lower():
                content = self._get_element_text(item, "encoded", ns_uri)
                if content:
                    break

        # Get categories
        categories = []
        for cat in item.findall("category"):
            if cat.text:
                categories.append(cat.text.strip())

        # Try Dublin Core for author
        if not author:
            for ns_prefix, ns_uri in namespaces.items():
                if "dc" in ns_prefix.lower():
                    author = self._get_element_text(item, "creator", ns_uri)
                    if author:
                        break

        return {
            "guid": guid or link or self.compute_checksum({"title": title, "link": link}),
            "title": title,
            "link": link,
            "description": description,
            "content": content,
            "pubDate": pub_date,
            "author": author,
            "categories": categories,
        }

    def _fetch_feed(self) -> str:
        """Fetch RSS feed content."""
        self.rate_limiter.wait_if_needed("rss")
        self.rate_limiter.record_request("rss")

        headers = {
            "User-Agent": "Kaspero ETL Bot/1.0",
            "Accept": "application/rss+xml, application/xml, text/xml",
        }

        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                response = client.get(self.feed_url, headers=headers)
                response.raise_for_status()
                self.rate_limiter.record_success("rss")
                return response.text

        except httpx.HTTPError as e:
            logger.error(f"RSS feed fetch failed: {e}")
            self.rate_limiter.record_failure("rss")
            raise ExtractionError(f"RSS feed fetch failed: {e}")

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Extract data from RSS feed."""
        try:
            feed_content = self._fetch_feed()

            # Parse XML
            root = ET.fromstring(feed_content)

            # Get namespaces
            namespaces = (
                dict(
                    [
                        node
                        for _, node in ET.iterparse(
                            (
                                self.feed_url
                                if self.feed_url.startswith("file:")
                                else None or self._fetch_feed()
                            ),  # This will use cache
                            events=["start-ns"],
                        )
                    ]
                )
                if False
                else {}
            )  # Simplified namespace handling

            # Try to find namespaces from root
            namespaces = {}
            for key, value in root.attrib.items():
                if key.startswith("{"):
                    continue
                if "xmlns" in key:
                    prefix = key.split(":")[-1] if ":" in key else ""
                    namespaces[prefix] = value

            # Find channel (RSS 2.0) or feed (Atom)
            channel = root.find("channel")
            if channel is None:
                channel = root  # Might be Atom format

            # Get checkpoint for incremental ingestion
            checkpoint = self.checkpoint_manager.get_checkpoint(self.source_type)
            last_guid = checkpoint.last_source_id if checkpoint else None

            # Parse items
            items = channel.findall("item")
            if not items:
                items = root.findall(".//{http://www.w3.org/2005/Atom}entry")

            for item in items:
                try:
                    parsed = self._parse_item(item, namespaces)

                    # Check if already processed (incremental)
                    if last_guid and parsed.get("guid") == last_guid:
                        logger.info(f"Reached last processed item: {last_guid}")
                        break

                    yield parsed

                except Exception as e:
                    logger.error(f"Error parsing RSS item: {e}")
                    self.records_failed += 1
                    continue

        except ET.ParseError as e:
            logger.error(f"Error parsing RSS XML: {e}")
            raise ExtractionError(f"RSS parsing failed: {e}")
        except Exception as e:
            logger.error(f"Error during RSS extraction: {e}")
            raise ExtractionError(f"RSS extraction failed: {e}")

    def get_source_id(self, raw_data: Dict[str, Any]) -> str:
        """Get unique source ID from RSS data."""
        return str(raw_data.get("guid", self.compute_checksum(raw_data)))

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform RSS data to unified schema."""
        # Parse publication date
        published_at = self._parse_date(raw_data.get("pubDate"))

        # Clean HTML from description and content
        description = self._strip_html(raw_data.get("description", ""))
        content = self._strip_html(raw_data.get("content", "")) or description

        # Get categories as tags
        tags = raw_data.get("categories", [])
        if not tags and raw_data.get("category"):
            tags = [raw_data["category"]]

        return {
            "title": raw_data.get("title"),
            "description": description[:500] if description else None,
            "content": content,
            "author": raw_data.get("author"),
            "category": tags[0] if tags else None,
            "tags": tags if tags else None,
            "url": raw_data.get("link"),
            "published_at": published_at,
            "extra_data": {
                "guid": raw_data.get("guid"),
                "feed_url": self.feed_url,
            },
        }

    def load_raw(self, raw_data: Dict[str, Any]) -> int:
        """Load raw RSS data with upsert (idempotent)."""
        source_id = self.get_source_id(raw_data)
        checksum = self.compute_checksum(raw_data)

        stmt = (
            insert(RawRSSData)
            .values(
                source_id=source_id,
                raw_payload=raw_data,
                feed_url=self.feed_url,
                checksum=checksum,
                ingested_at=datetime.utcnow(),
            )
            .on_conflict_do_update(
                index_elements=["source_id"],
                set_={
                    "raw_payload": raw_data,
                    "checksum": checksum,
                    "ingested_at": datetime.utcnow(),
                },
            )
            .returning(RawRSSData.id)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        return result.scalar_one()

    def load_unified(self, transformed_data: Dict[str, Any], raw_id: int) -> int:
        """Load transformed data into unified table with upsert."""
        source_id = str(raw_id)

        stmt = (
            insert(UnifiedData)
            .values(
                source_type=self.source_type, source_id=source_id, raw_id=raw_id, **transformed_data
            )
            .on_conflict_do_update(
                constraint="uq_unified_source",
                set_={"raw_id": raw_id, "updated_at": datetime.utcnow(), **transformed_data},
            )
            .returning(UnifiedData.id)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        return result.scalar_one()

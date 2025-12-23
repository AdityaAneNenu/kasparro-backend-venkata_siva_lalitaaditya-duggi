"""CoinGecko API data extractor."""

import logging
import time
from datetime import datetime
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


class CoinGeckoExtractor(BaseExtractor):
    """Extractor for CoinGecko API (free, no API key required).

    This uses the RSS/secondary source type to maintain compatibility
    with the existing data model and provide a second data source.

    CoinGecko API Documentation: https://www.coingecko.com/en/api/documentation
    """

    source_type = SourceType.RSS  # Using RSS source type for second API

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(
        self,
        db: Session,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ):
        super().__init__(db, rate_limiter, **kwargs)
        self.feed_url = f"{self.BASE_URL}/coins/markets"

    def _make_request(
        self,
        endpoint: str = "/coins/markets",
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make API request with rate limiting."""
        url = f"{self.BASE_URL}{endpoint}"

        # Rate limiting - CoinGecko free tier: 10-30 calls/minute
        self.rate_limiter.wait_if_needed("coingecko")
        self.rate_limiter.record_request("coingecko")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url, headers=headers, params=params)

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 60))
                    backoff = self.rate_limiter.record_failure("coingecko")
                    logger.warning(f"Rate limited, sleeping {max(backoff, retry_after)}s")
                    time.sleep(max(backoff, retry_after))
                    return self._make_request(endpoint, params)

                response.raise_for_status()
                self.rate_limiter.record_success("coingecko")

                return response.json()

        except httpx.HTTPError as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise ExtractionError(f"CoinGecko API request failed: {e}")

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Extract cryptocurrency market data from CoinGecko."""
        # Get checkpoint for incremental ingestion
        checkpoint = self.checkpoint_manager.get_checkpoint(self.source_type)

        page = 1
        per_page = 100
        has_more = True

        while has_more:
            try:
                params = {
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": per_page,
                    "page": page,
                    "sparkline": "false",
                    "price_change_percentage": "24h,7d,30d",
                }

                response = self._make_request("/coins/markets", params)

                if not response or not isinstance(response, list):
                    has_more = False
                    continue

                for coin in response:
                    # Add metadata
                    coin["_fetched_at"] = datetime.utcnow().isoformat()
                    coin["_source"] = "coingecko"
                    yield coin

                page += 1

                # Check if we've received less than a full page
                if len(response) < per_page:
                    has_more = False

                # Limit to first 3 pages (300 coins) to stay within rate limits
                if page > 3:
                    has_more = False

            except Exception as e:
                logger.error(f"Error during CoinGecko extraction: {e}")
                raise ExtractionError(f"CoinGecko extraction failed: {e}")

    def get_source_id(self, raw_data: Dict[str, Any]) -> str:
        """Get unique source ID from CoinGecko data."""
        # CoinGecko provides unique coin ID
        coin_id = raw_data.get("id", "")
        # Include date for daily snapshots
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        return f"coingecko:{coin_id}:{date_str}"

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CoinGecko data to unified schema."""
        # Parse last updated time
        published_at = None
        last_updated = raw_data.get("last_updated")
        if last_updated:
            try:
                published_at = datetime.fromisoformat(str(last_updated).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                published_at = datetime.utcnow()

        # Build description with market data
        price = raw_data.get("current_price", 0)
        market_cap = raw_data.get("market_cap", 0)
        volume = raw_data.get("total_volume", 0)
        change_24h = raw_data.get("price_change_percentage_24h", 0)

        description = (
            f"Current Price: ${price:,.2f} | "
            f"24h Change: {change_24h:+.2f}% | "
            f"Market Cap: ${market_cap:,.0f} | "
            f"24h Volume: ${volume:,.0f}"
        )

        # Create tags from categories
        tags = []
        if raw_data.get("market_cap_rank"):
            tags.append(f"rank-{raw_data['market_cap_rank']}")
        if change_24h and change_24h > 0:
            tags.append("bullish")
        elif change_24h and change_24h < 0:
            tags.append("bearish")

        return {
            "title": f"{raw_data.get('name', 'Unknown')} ({raw_data.get('symbol', '').upper()})",
            "description": description,
            "content": str(raw_data),  # Full data as content
            "author": "CoinGecko",
            "category": "cryptocurrency",
            "tags": tags if tags else None,
            "url": f"https://www.coingecko.com/en/coins/{raw_data.get('id', '')}",
            "published_at": published_at,
            "extra_data": {
                "coin_id": raw_data.get("id"),
                "symbol": raw_data.get("symbol"),
                "current_price": price,
                "market_cap": market_cap,
                "market_cap_rank": raw_data.get("market_cap_rank"),
                "total_volume": volume,
                "high_24h": raw_data.get("high_24h"),
                "low_24h": raw_data.get("low_24h"),
                "price_change_24h": raw_data.get("price_change_24h"),
                "price_change_percentage_24h": change_24h,
                "price_change_percentage_7d": raw_data.get(
                    "price_change_percentage_7d_in_currency"
                ),
                "price_change_percentage_30d": raw_data.get(
                    "price_change_percentage_30d_in_currency"
                ),
                "circulating_supply": raw_data.get("circulating_supply"),
                "total_supply": raw_data.get("total_supply"),
                "ath": raw_data.get("ath"),
                "ath_change_percentage": raw_data.get("ath_change_percentage"),
                "atl": raw_data.get("atl"),
                "image": raw_data.get("image"),
            },
        }

    def load_raw(self, raw_data: Dict[str, Any]) -> int:
        """Load raw CoinGecko data with upsert (idempotent)."""
        source_id = self.get_source_id(raw_data)
        checksum = self.compute_checksum(raw_data)

        # Upsert for idempotency
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

"""CoinPaprika API data extractor."""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Generator, Optional

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import get_settings
from core.exceptions import AuthenticationError, ExtractionError
from core.models import RawAPIData, SourceType, UnifiedData
from ingestion.base import BaseExtractor
from services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
settings = get_settings()


class APIExtractor(BaseExtractor):
    """Extractor for CoinPaprika API data source.

    CoinPaprika API Documentation: https://api.coinpaprika.com/
    Free tier available, API key recommended for higher rate limits.
    """

    source_type = SourceType.API

    BASE_URL = "https://api.coinpaprika.com/v1"

    def __init__(
        self,
        db: Session,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ):
        super().__init__(db, rate_limiter, **kwargs)
        self.api_url = api_url or self.BASE_URL
        self.api_key = api_key or settings.API_KEY

        if not self.api_key:
            logger.info("No API key configured - using CoinPaprika free tier")

    def _make_request(
        self,
        endpoint: str = "/coins",
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make an API request with rate limiting."""
        url = f"{self.api_url}{endpoint}"

        # Rate limiting
        self.rate_limiter.wait_if_needed("api")
        self.rate_limiter.record_request("api")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Add API key if available (for higher rate limits)
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url, headers=headers, params=params if params else None)

                if response.status_code == 401:
                    raise AuthenticationError("API authentication failed")
                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 60))
                    backoff = self.rate_limiter.record_failure("api")
                    logger.warning(f"Rate limited, sleeping {max(backoff, retry_after)}s")
                    time.sleep(max(backoff, retry_after))
                    return self._make_request(endpoint, params)

                response.raise_for_status()
                self.rate_limiter.record_success("api")

                return response.json()

        except httpx.HTTPError as e:
            logger.error(f"API request failed: {e}")
            raise ExtractionError(f"API request failed: {e}")

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Extract cryptocurrency data from CoinPaprika."""
        # Get checkpoint for incremental ingestion
        checkpoint = self.checkpoint_manager.get_checkpoint(self.source_type)

        try:
            # First, get list of coins
            coins = self._make_request("/coins")

            if not isinstance(coins, list):
                logger.error("Unexpected response format from CoinPaprika")
                return

            # Get top coins by rank (limit to 100 for rate limiting)
            active_coins = [c for c in coins if c.get("is_active", False)][:100]

            for coin in active_coins:
                coin_id = coin.get("id")
                if not coin_id:
                    continue

                # Get detailed ticker data for each coin
                try:
                    ticker = self._make_request(f"/tickers/{coin_id}")
                    if ticker:
                        # Merge coin info with ticker data
                        merged_data = {**coin, **ticker}
                        merged_data["_fetched_at"] = datetime.utcnow().isoformat()
                        merged_data["_source"] = "coinpaprika"
                        yield merged_data
                except Exception as e:
                    logger.warning(f"Failed to get ticker for {coin_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error during CoinPaprika extraction: {e}")
            raise ExtractionError(f"CoinPaprika extraction failed: {e}")

    def get_source_id(self, raw_data: Dict[str, Any]) -> str:
        """Get unique source ID from CoinPaprika data."""
        # CoinPaprika provides unique coin ID
        coin_id = raw_data.get("id", "")
        # Include date for daily snapshots
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        return f"coinpaprika:{coin_id}:{date_str}"

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CoinPaprika data to unified schema."""
        # Parse last updated time
        published_at = None
        last_updated = raw_data.get("last_updated")
        if last_updated:
            try:
                published_at = datetime.fromisoformat(str(last_updated).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                published_at = datetime.utcnow()
        else:
            published_at = datetime.utcnow()

        # Extract price data from quotes
        quotes = raw_data.get("quotes", {})
        usd_data = quotes.get("USD", {})

        price = usd_data.get("price", 0)
        market_cap = usd_data.get("market_cap", 0)
        volume_24h = usd_data.get("volume_24h", 0)
        change_24h = usd_data.get("percent_change_24h", 0)
        change_7d = usd_data.get("percent_change_7d", 0)
        change_30d = usd_data.get("percent_change_30d", 0)

        # Build description with market data
        description = (
            f"Current Price: ${price:,.6f} | "
            f"24h Change: {change_24h:+.2f}% | "
            f"Market Cap: ${market_cap:,.0f} | "
            f"24h Volume: ${volume_24h:,.0f}"
        )

        # Create tags based on performance
        tags = []
        rank = raw_data.get("rank")
        if rank:
            tags.append(f"rank-{rank}")
        if change_24h and change_24h > 0:
            tags.append("bullish")
        elif change_24h and change_24h < 0:
            tags.append("bearish")
        if raw_data.get("is_new"):
            tags.append("new-listing")

        return {
            "title": f"{raw_data.get('name', 'Unknown')} ({raw_data.get('symbol', '').upper()})",
            "description": description,
            "content": str(raw_data),  # Full data as content
            "author": "CoinPaprika",
            "category": "cryptocurrency",
            "tags": tags if tags else None,
            "url": f"https://coinpaprika.com/coin/{raw_data.get('id', '')}",
            "published_at": published_at,
            "extra_data": {
                "coin_id": raw_data.get("id"),
                "symbol": raw_data.get("symbol"),
                "rank": rank,
                "current_price": price,
                "market_cap": market_cap,
                "volume_24h": volume_24h,
                "percent_change_1h": usd_data.get("percent_change_1h"),
                "percent_change_24h": change_24h,
                "percent_change_7d": change_7d,
                "percent_change_30d": change_30d,
                "circulating_supply": raw_data.get("circulating_supply"),
                "total_supply": raw_data.get("total_supply"),
                "max_supply": raw_data.get("max_supply"),
                "ath_price": usd_data.get("ath_price"),
                "ath_date": usd_data.get("ath_date"),
                "is_active": raw_data.get("is_active"),
                "is_new": raw_data.get("is_new"),
            },
        }

    def load_raw(self, raw_data: Dict[str, Any]) -> int:
        """Load raw API data with upsert (idempotent)."""
        source_id = self.get_source_id(raw_data)
        checksum = self.compute_checksum(raw_data)

        # Upsert for idempotency
        stmt = (
            insert(RawAPIData)
            .values(
                source_id=source_id,
                raw_payload=raw_data,
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
            .returning(RawAPIData.id)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        return result.scalar_one()

    def load_unified(self, transformed_data: Dict[str, Any], raw_id: int) -> int:
        """Load transformed data into unified table with upsert."""
        source_id = str(raw_id)  # Use raw_id as source reference

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

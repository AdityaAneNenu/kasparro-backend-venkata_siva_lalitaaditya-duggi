"""Tests for ETL transformation logic."""

from datetime import datetime

import pytest

from core.models import SourceType
from ingestion.api_extractor import APIExtractor
from ingestion.csv_extractor import CSVExtractor
from ingestion.rss_extractor import RSSExtractor


class TestCSVTransformation:
    """Test CSV data transformation."""

    def test_transform_basic_fields(self, db_session):
        """Test basic field mapping."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "title": "Test Title",
            "description": "Test Description",
            "author": "Test Author",
            "category": "Test Category",
            "date": "2024-01-15",
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        result = extractor.transform(raw_data)

        assert result["title"] == "Test Title"
        assert result["description"] == "Test Description"
        assert result["author"] == "Test Author"
        assert result["category"] == "Test Category"
        assert result["published_at"] == datetime(2024, 1, 15)

    def test_transform_alternative_field_names(self, db_session):
        """Test mapping of alternative field names."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "name": "Name as Title",
            "summary": "Summary as Description",
            "creator": "Creator as Author",
            "type": "Type as Category",
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        result = extractor.transform(raw_data)

        assert result["title"] == "Name as Title"
        assert result["description"] == "Summary as Description"
        assert result["author"] == "Creator as Author"
        assert result["category"] == "Type as Category"

    def test_transform_date_formats(self, db_session):
        """Test parsing of different date formats."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        date_formats = [
            ("2024-01-15", datetime(2024, 1, 15)),
            ("15/01/2024", datetime(2024, 1, 15)),
            ("01/15/2024", datetime(2024, 1, 15)),
            ("2024/01/15", datetime(2024, 1, 15)),
        ]

        for date_str, expected in date_formats:
            raw_data = {"date": date_str, "_row_number": 1, "_source_file": "test.csv"}
            result = extractor.transform(raw_data)
            assert result["published_at"] == expected, f"Failed for {date_str}"

    def test_transform_tags_from_string(self, db_session):
        """Test parsing comma-separated tags."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "title": "Test",
            "tags": "tag1, tag2, tag3",
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        result = extractor.transform(raw_data)

        assert result["tags"] == ["tag1", "tag2", "tag3"]

    def test_transform_extra_data_collection(self, db_session):
        """Test that unmapped fields go to extra_data."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "title": "Test",
            "custom_field": "Custom Value",
            "another_field": 123,
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        result = extractor.transform(raw_data)

        assert result["extra_data"]["custom_field"] == "Custom Value"
        assert result["extra_data"]["another_field"] == 123


class TestAPITransformation:
    """Test API (CoinPaprika) data transformation."""

    def test_transform_coinpaprika_response(self, db_session):
        """Test transformation of CoinPaprika API response."""
        extractor = APIExtractor(db=db_session)

        raw_data = {
            "id": "btc-bitcoin",
            "name": "Bitcoin",
            "symbol": "BTC",
            "rank": 1,
            "is_active": True,
            "quotes": {
                "USD": {
                    "price": 43000.50,
                    "market_cap": 850000000000,
                    "volume_24h": 25000000000,
                    "percent_change_1h": 0.5,
                    "percent_change_24h": 2.3,
                    "percent_change_7d": -1.2,
                    "percent_change_30d": 5.5,
                }
            },
            "last_updated": "2024-01-15T10:30:00Z",
        }

        result = extractor.transform(raw_data)

        assert "Bitcoin" in result["title"]
        assert "BTC" in result["title"]
        assert result["category"] == "cryptocurrency"
        assert result["author"] == "CoinPaprika"
        assert result["published_at"] is not None
        assert "rank-1" in result["tags"]
        assert "current_price" in result["extra_data"]

    def test_transform_bullish_coin(self, db_session):
        """Test that bullish coins get appropriate tag."""
        extractor = APIExtractor(db=db_session)

        raw_data = {
            "id": "eth-ethereum",
            "name": "Ethereum",
            "symbol": "ETH",
            "rank": 2,
            "quotes": {
                "USD": {
                    "price": 2500.00,
                    "percent_change_24h": 5.5,  # Positive = bullish
                }
            },
        }

        result = extractor.transform(raw_data)

        assert "bullish" in result["tags"]

    def test_transform_bearish_coin(self, db_session):
        """Test that bearish coins get appropriate tag."""
        extractor = APIExtractor(db=db_session)

        raw_data = {
            "id": "sol-solana",
            "name": "Solana",
            "symbol": "SOL",
            "rank": 5,
            "quotes": {
                "USD": {
                    "price": 100.00,
                    "percent_change_24h": -3.5,  # Negative = bearish
                }
            },
        }

        result = extractor.transform(raw_data)

        assert "bearish" in result["tags"]

    def test_transform_description_format(self, db_session):
        """Test that description includes market data."""
        extractor = APIExtractor(db=db_session)

        raw_data = {
            "id": "doge-dogecoin",
            "name": "Dogecoin",
            "symbol": "DOGE",
            "quotes": {
                "USD": {
                    "price": 0.08,
                    "market_cap": 10000000000,
                    "volume_24h": 500000000,
                    "percent_change_24h": 1.5,
                }
            },
        }

        result = extractor.transform(raw_data)

        assert "Current Price" in result["description"]
        assert "Market Cap" in result["description"]


class TestCoinGeckoTransformation:
    """Test CoinGecko data transformation."""

    def test_transform_coingecko_response(self, db_session):
        """Test transformation of CoinGecko API response."""
        from ingestion.coingecko_extractor import CoinGeckoExtractor

        extractor = CoinGeckoExtractor(db=db_session)

        raw_data = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 43000.50,
            "market_cap": 850000000000,
            "market_cap_rank": 1,
            "total_volume": 25000000000,
            "high_24h": 44000,
            "low_24h": 42000,
            "price_change_24h": 500,
            "price_change_percentage_24h": 1.5,
            "last_updated": "2024-01-15T10:30:00Z",
        }

        result = extractor.transform(raw_data)

        assert "Bitcoin" in result["title"]
        assert "BTC" in result["title"]
        assert result["category"] == "cryptocurrency"
        assert result["author"] == "CoinGecko"
        assert result["published_at"] is not None
        assert "rank-1" in result["tags"]

    def test_transform_coingecko_bullish(self, db_session):
        """Test CoinGecko bullish coin tagging."""
        from ingestion.coingecko_extractor import CoinGeckoExtractor

        extractor = CoinGeckoExtractor(db=db_session)

        raw_data = {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2500,
            "market_cap_rank": 2,
            "price_change_percentage_24h": 5.0,
        }

        result = extractor.transform(raw_data)

        assert "bullish" in result["tags"]


class TestRSSTransformation:
    """Test RSS data transformation (kept for backwards compatibility)."""

    def test_transform_rss_item(self, db_session):
        """Test transformation of RSS item."""
        extractor = RSSExtractor(db=db_session, feed_url="https://example.com/feed")

        raw_data = {
            "guid": "rss-123",
            "title": "RSS Title",
            "description": "<p>HTML description</p>",
            "link": "https://example.com/article",
            "author": "RSS Author",
            "pubDate": "Mon, 15 Jan 2024 10:00:00 +0000",
            "categories": ["Tech", "News"],
        }

        result = extractor.transform(raw_data)

        assert result["title"] == "RSS Title"
        assert "<p>" not in result["description"]  # HTML tags should be stripped
        assert "HTML description" in result["description"]  # Text content preserved
        assert result["url"] == "https://example.com/article"
        assert result["author"] == "RSS Author"
        assert result["published_at"] is not None
        assert result["tags"] == ["Tech", "News"]

    def test_strip_html_from_description(self, db_session):
        """Test HTML stripping from RSS content."""
        extractor = RSSExtractor(db=db_session, feed_url="https://example.com/feed")

        html_text = "<p>This is <b>bold</b> and <a href='#'>linked</a> text.</p>"
        result = extractor._strip_html(html_text)

        assert "<" not in result
        assert ">" not in result
        assert "This is bold and linked text." in result

    def test_parse_rss_date_formats(self, db_session):
        """Test parsing of RSS date formats."""
        extractor = RSSExtractor(db=db_session, feed_url="https://example.com/feed")

        # RFC 2822 format
        result = extractor._parse_date("Mon, 15 Jan 2024 10:00:00 +0000")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15


class TestSourceIdGeneration:
    """Test source ID generation for different extractors."""

    def test_csv_source_id(self, db_session):
        """Test CSV source ID generation."""
        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "_row_number": 42,
            "_source_file": "data.csv",
        }

        source_id = extractor.get_source_id(raw_data)

        assert source_id == "data.csv:42"

    def test_api_source_id_from_id_field(self, db_session):
        """Test API source ID from id field."""
        from datetime import datetime

        extractor = APIExtractor(db=db_session)

        raw_data = {"id": "api-unique-123"}
        source_id = extractor.get_source_id(raw_data)

        # CoinPaprika format: coinpaprika:{id}:{date}
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        assert source_id == f"coinpaprika:api-unique-123:{date_str}"

    def test_api_source_id_fallback_to_checksum(self, db_session):
        """Test API source ID falls back to empty id with date when no id field."""
        from datetime import datetime

        extractor = APIExtractor(db=db_session)

        raw_data = {"title": "No ID Field"}
        source_id = extractor.get_source_id(raw_data)

        # CoinPaprika format with empty id: coinpaprika::{date}
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        assert source_id == f"coinpaprika::{date_str}"

    def test_rss_source_id_from_guid(self, db_session):
        """Test RSS source ID from guid."""
        extractor = RSSExtractor(db=db_session, feed_url="https://example.com/feed")

        raw_data = {"guid": "rss-unique-456"}
        source_id = extractor.get_source_id(raw_data)

        assert source_id == "rss-unique-456"

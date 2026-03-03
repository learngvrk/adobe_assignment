"""Shared fixtures for the test suite."""

import sys
from pathlib import Path

import pytest

# Allow imports from src/
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.config import load_config
from common.analyzer import SessionAwareAnalyzer

COLUMNS = [
    "hit_time_gmt", "date_time", "user_agent", "ip", "event_list",
    "geo_city", "geo_region", "geo_country", "pagename", "page_url",
    "product_list", "referrer",
]


def make_tsv(rows: list[dict]) -> str:
    """Build a TSV string from a list of row dicts (missing keys default to '')."""
    header = "\t".join(COLUMNS)
    lines = [header]
    for row in rows:
        lines.append("\t".join(str(row.get(c, "")) for c in COLUMNS))
    return "\n".join(lines) + "\n"


@pytest.fixture
def config():
    return {
        "site_domain": "esshopzilla.com",
        "search_engine_domains": ["google", "bing", "yahoo", "duckduckgo", "msn", "ask", "aol"],
        "keyword_params": ["q", "p", "query", "search"],
    }


@pytest.fixture
def analyzer(config):
    return SessionAwareAnalyzer(config)

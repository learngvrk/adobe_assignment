"""Tests for src/common/analyzer.py — SessionAwareAnalyzer"""

from datetime import datetime
from pathlib import Path

import pytest

from common.analyzer import SessionAwareAnalyzer
from conftest import make_tsv
from common.config import load_config


config = load_config()

# ---------- Static helper tests ----------

class TestIsPurchase:
    def test_purchase_event(self):
        assert SessionAwareAnalyzer._is_purchase("1") is True

    def test_purchase_among_others(self):
        assert SessionAwareAnalyzer._is_purchase("2,1") is True

    def test_non_purchase_event(self):
        assert SessionAwareAnalyzer._is_purchase("2") is False

    def test_empty_string(self):
        assert SessionAwareAnalyzer._is_purchase("") is False

    def test_none(self):
        assert SessionAwareAnalyzer._is_purchase(None) is False

    def test_event_11_is_not_purchase(self):
        """Event '11' should not match event '1'."""
        assert SessionAwareAnalyzer._is_purchase("11") is False

    def test_event_12_is_not_purchase(self):
        assert SessionAwareAnalyzer._is_purchase("12") is False


class TestExtractRevenue:
    def test_single_product(self):
        assert SessionAwareAnalyzer._extract_revenue("Electronics;Ipod;1;290;") == 290.0

    def test_multi_product(self):
        product_list = "Electronics;A;1;100;,Electronics;B;1;50;"
        assert SessionAwareAnalyzer._extract_revenue(product_list) == 150.0

    def test_no_revenue_field(self):
        assert SessionAwareAnalyzer._extract_revenue("A;B") == 0.0

    def test_empty_string(self):
        assert SessionAwareAnalyzer._extract_revenue("") == 0.0

    def test_none(self):
        assert SessionAwareAnalyzer._extract_revenue(None) == 0.0

    def test_empty_revenue_value(self):
        assert SessionAwareAnalyzer._extract_revenue("Electronics;Ipod;1;;") == 0.0


# ---------- End-to-end process tests ----------

class TestProcess:
    def test_sample_data(self, analyzer):
        """Integration test against the real sample data file."""
        sample = Path(__file__).parent.parent / config.get("Input_folder", "Input_data") / config.get("data_file", "data.sql")
        with open(sample) as f:
            content = f.read()

        results = analyzer.process(content)

        assert len(results) == 2
        assert results[0] == {
            "Search Engine Domain": "google.com",
            "Search Keyword": "ipod",
            "Revenue": 480.0,
        }
        assert results[1] == {
            "Search Engine Domain": "bing.com",
            "Search Keyword": "zune",
            "Revenue": 250.0,
        }

    def test_no_purchase(self, analyzer):
        """Search entry but no purchase event → empty results."""
        tsv = make_tsv([
            {
                "hit_time_gmt": "1000000",
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "http://www.google.com/search?q=shoes",
                "event_list": "2",
                "product_list": "",
            },
        ])
        assert analyzer.process(tsv) == []

    def test_no_search_referrer(self, analyzer):
        """Direct visit with purchase → empty (no search attribution)."""
        tsv = make_tsv([
            {
                "hit_time_gmt": "1000000",
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "",
                "event_list": "1",
                "product_list": "Electronics;Widget;1;99;",
            },
        ])
        assert analyzer.process(tsv) == []

    def test_session_timeout_breaks_attribution(self, analyzer):
        """Search entry, then 31-minute gap, then purchase → no attribution."""
        t1 = 1000000
        t2 = t1 + 1900  # 31+ minutes later (> 1800s)
        tsv = make_tsv([
            {
                "hit_time_gmt": str(t1),
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "http://www.google.com/search?q=shoes",
                "event_list": "",
                "product_list": "",
            },
            {
                "hit_time_gmt": str(t2),
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "http://www.esshopzilla.com/cart/",
                "event_list": "1",
                "product_list": "Electronics;Shoes;1;120;",
            },
        ])
        assert analyzer.process(tsv) == []

    def test_within_session_attributes_correctly(self, analyzer):
        """Search entry, then purchase within 30 min → attributed."""
        t1 = 1000000
        t2 = t1 + 600  # 10 minutes later
        tsv = make_tsv([
            {
                "hit_time_gmt": str(t1),
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "http://www.bing.com/search?q=tablet",
                "event_list": "",
                "product_list": "",
            },
            {
                "hit_time_gmt": str(t2),
                "ip": "1.2.3.4",
                "user_agent": "TestBot",
                "referrer": "http://www.esshopzilla.com/product/",
                "event_list": "1",
                "product_list": "Electronics;Tablet;1;350;",
            },
        ])
        results = analyzer.process(tsv)
        assert len(results) == 1
        assert results[0]["Search Engine Domain"] == "bing.com"
        assert results[0]["Search Keyword"] == "tablet"
        assert results[0]["Revenue"] == 350.0

    def test_multiple_sessions_same_visitor(self, analyzer):
        """Two sessions (separated by timeout) with different keywords → both attributed."""
        t1 = 1000000
        t2 = t1 + 300       # browse within session 1
        t3 = t1 + 600       # purchase within session 1
        t4 = t1 + 5000      # new session (>30 min gap)
        t5 = t4 + 300       # purchase within session 2
        tsv = make_tsv([
            {
                "hit_time_gmt": str(t1),
                "ip": "5.6.7.8",
                "user_agent": "Bot",
                "referrer": "http://www.google.com/search?q=laptop",
                "event_list": "",
                "product_list": "",
            },
            {
                "hit_time_gmt": str(t2),
                "ip": "5.6.7.8",
                "user_agent": "Bot",
                "referrer": "http://www.esshopzilla.com/",
                "event_list": "",
                "product_list": "",
            },
            {
                "hit_time_gmt": str(t3),
                "ip": "5.6.7.8",
                "user_agent": "Bot",
                "referrer": "http://www.esshopzilla.com/cart/",
                "event_list": "1",
                "product_list": "Electronics;Laptop;1;800;",
            },
            {
                "hit_time_gmt": str(t4),
                "ip": "5.6.7.8",
                "user_agent": "Bot",
                "referrer": "http://www.bing.com/search?q=mouse",
                "event_list": "",
                "product_list": "",
            },
            {
                "hit_time_gmt": str(t5),
                "ip": "5.6.7.8",
                "user_agent": "Bot",
                "referrer": "http://www.esshopzilla.com/product/",
                "event_list": "1",
                "product_list": "Electronics;Mouse;1;50;",
            },
        ])
        results = analyzer.process(tsv)
        assert len(results) == 2
        domains = {r["Search Engine Domain"] for r in results}
        assert domains == {"google.com", "bing.com"}

    def test_empty_input(self, analyzer):
        """Header-only TSV → empty results."""
        tsv = make_tsv([])
        assert analyzer.process(tsv) == []


# ---------- to_tab_delimited ----------

class TestToTabDelimited:
    def test_filename_format(self, analyzer):
        dt = datetime(2024, 3, 15)
        results = [{"Search Engine Domain": "google.com", "Search Keyword": "test", "Revenue": 100.0}]
        filename, _ = analyzer.to_tab_delimited(results, execution_date=dt)
        assert filename == "2024-03-15_SearchKeywordPerformance.tab"

    def test_tab_content(self, analyzer):
        results = [{"Search Engine Domain": "google.com", "Search Keyword": "ipod", "Revenue": 480.0}]
        _, content = analyzer.to_tab_delimited(results)
        lines = content.strip().split("\n")
        assert lines[0] == "Search Engine Domain\tSearch Keyword\tRevenue"
        assert lines[1] == "google.com\tipod\t480.00"

    def test_empty_results(self, analyzer):
        _, content = analyzer.to_tab_delimited([])
        assert "Search Engine Domain\tSearch Keyword\tRevenue" in content
        lines = content.strip().split("\n")
        assert len(lines) == 1  # header only

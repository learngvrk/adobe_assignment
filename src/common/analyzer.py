"""
SessionAwareAnalyzer
--------------------
Session-level first-touch attribution of e-commerce revenue to external
search engine keywords from Adobe Analytics hit-level TSV data.

Session rules:
    - Session key   : ip + user_agent
    - Session break : inactivity gap > 30 minutes between consecutive hits
    - First-touch   : the first search engine referrer seen in a session is
                      locked in and attributed to all purchases in that session

Why need session-aware?
    The search keyword attribution is session-level, not row-level.
    The originating search engine referrer lives on the session entry hit, not the purchase hit.
    Row-level attribution produces zero results on this dataset.
"""

import io
import logging
from datetime import datetime
from typing import Optional

import polars as pl

from .url_parser import parse_domain, parse_keyword

logger = logging.getLogger(__name__)

SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes — industry standard


class SessionAwareAnalyzer:
    """
    Parses Adobe Analytics hit-level TSV data

    Framework-agnostic: used directly by the Lambda handler (small files)
    and as the domain-logic layer by the EMR Serverless PySpark job (large files).
    """

    OUTPUT_COLUMNS = ["Search Engine Domain", "Search Keyword", "Revenue"]

    def __init__(self, config: dict):
        """
        Args:
            config: Loaded from search_engines.toml via config.load_config().
                    Expected keys: search_engine_domains, keyword_params, site_domain.
        """
        self.domains: list[str] = config["search_engine_domains"]
        self.keyword_params: list[str] = config["keyword_params"]
        self.site_domain: str = config.get("site_domain", "")

    # ------------------------------------------------------------------
    # Public Methods

    def process(self, tsv_content: str) -> list[dict]:
        """
        Parse a full TSV string and return aggregated revenue records.

        Args:
            tsv_content: Raw tab-separated content of the hit-level data file.

        Returns:
            List of dicts with keys: Search Engine Domain, Search Keyword,
            Revenue — sorted by Revenue descending.
        """
        df = pl.read_csv(
            io.StringIO(tsv_content),
            separator="\t",
            infer_schema=False,
        )
        attributed = self._attribute_sessions(df)
        return self._aggregate_and_sort(attributed)

    def to_tab_delimited(
        self,
        results: list[dict],
        execution_date: Optional[datetime] = None,
    ) -> tuple[str, str]:
        """
        Serialize results to a tab-delimited string and generate the output
        filename: YYYY-mm-dd_SearchKeywordPerformance.tab
        """
        date = execution_date or datetime.now(tz=None)
        filename = f"{date.strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

        if not results:
            header = "\t".join(self.OUTPUT_COLUMNS) + "\n"
            return filename, header

        df = pl.DataFrame(results, schema=self.OUTPUT_COLUMNS)
        df = df.with_columns(
            pl.col("Revenue").map_elements(
                lambda v: f"{v:.2f}", return_dtype=pl.Utf8
            )
        )
        content = df.write_csv(separator="\t", line_terminator="\n")

        return filename, content

    # ------------------------------------------------------------------
    # Session attribution (polars)
    # ------------------------------------------------------------------

    def _attribute_sessions(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Core session logic — declarative Polars operations.

        1. Sort by (ip, user_agent, hit_time_gmt).
        2. Compute inactivity gaps per visitor.
        3. Detect session breaks (timeout > 30 min).
        4. Assign session IDs via cumulative sum of break flags.
        5. Extract first-touch search referrer per session.
        6. Join back to purchase rows and extract revenue.

        Returns:
            DataFrame with columns: Search Engine Domain, Search Keyword, Revenue
        """
        visitor = ["ip", "user_agent"]

        # Cast timestamp, sort by visitor + time
        df = (
            df.with_columns(
                pl.col("hit_time_gmt").cast(pl.Int64, strict=False).fill_null(0)
            )
            .sort(visitor + ["hit_time_gmt"])
        )

        # Inactivity gap between consecutive hits per visitor
        df = df.with_columns(
            pl.col("hit_time_gmt").diff().over(visitor).fill_null(0).alias("gap")
        )

        # Parse domain + keyword from referrer
        domains = self.domains
        keyword_params = self.keyword_params

        df = df.with_columns(
            pl.col("referrer").map_elements(
                lambda r: parse_domain(r, domains),
                return_dtype=pl.Utf8,
            ).alias("domain"),
            pl.col("referrer").map_elements(
                lambda r: parse_keyword(r, keyword_params),
                return_dtype=pl.Utf8,
            ).alias("keyword"),
        )

        # Session break: inactivity > 30 minutes
        df = df.with_columns(
            (pl.col("gap") > SESSION_TIMEOUT_SECONDS).alias("session_break")
        )

        # Session ID per visitor — cumulative sum of breaks
        df = df.with_columns(
            pl.col("session_break")
            .cast(pl.Int32)
            .cum_sum()
            .over(visitor)
            .alias("session_id")
        )

        session_key = visitor + ["session_id"]

        # First-touch: first search referrer per session
        first_touch = (
            df.filter(pl.col("domain").is_not_null() & pl.col("keyword").is_not_null())
            .group_by(session_key, maintain_order=True)
            .first()
            .select(
                session_key
                + [
                    pl.col("domain").alias("Search Engine Domain"),
                    pl.col("keyword").alias("Search Keyword"),
                ]
            )
        )

        if first_touch.is_empty():
            return pl.DataFrame(
                schema={
                    "Search Engine Domain": pl.Utf8,
                    "Search Keyword": pl.Utf8,
                    "Revenue": pl.Float64,
                }
            )

        # Purchase rows with revenue
        purchases = (
            df.with_columns(
                pl.col("event_list")
                .map_elements(self._is_purchase, return_dtype=pl.Boolean)
                .alias("is_purchase"),
                pl.col("product_list")
                .map_elements(self._extract_revenue, return_dtype=pl.Float64)
                .alias("Revenue"),
            )
            .filter(pl.col("is_purchase") & (pl.col("Revenue") > 0))
        )

        if purchases.is_empty():
            return pl.DataFrame(
                schema={
                    "Search Engine Domain": pl.Utf8,
                    "Search Keyword": pl.Utf8,
                    "Revenue": pl.Float64,
                }
            )

        # Join purchases to their session's first-touch
        attributed = purchases.join(first_touch, on=session_key, how="inner")

        return attributed.select(["Search Engine Domain", "Search Keyword", "Revenue"])

    # ------------------------------------------------------------------
    # Aggregation and sorting of the final results

    def _aggregate_and_sort(self, attributed: pl.DataFrame) -> list[dict]:
        """Aggregate revenue by (domain, keyword) and sort descending."""
        if attributed.is_empty():
            return []

        return (
            attributed.group_by(
                ["Search Engine Domain", "Search Keyword"], maintain_order=True
            )
            .agg(pl.col("Revenue").sum())
            .sort("Revenue", descending=True)
            .to_dicts()
        )

    # ------------------------------------------------------------------
    # helper functions for session attribution

    @staticmethod
    def _is_purchase(event_list: str) -> bool:
        """Return True if event_list contains the purchase event code '1'."""
        if not event_list:
            return False
        return "1" in [e.strip() for e in event_list.split(",")]

    @staticmethod
    def _extract_revenue(product_list: str) -> float:
        """
        Sum revenue across all products in a product_list string.

        Product list format (Appendix B):
            Category;Product Name;Qty;Revenue;CustomEvent,...
        Revenue is the 4th semicolon-delimited field (index 3).
        Multiple products are comma-delimited.
        """
        if not product_list:
            return 0.0

        total = 0.0
        for product in product_list.split(","):
            attrs = product.strip().split(";")
            if len(attrs) >= 4:
                try:
                    val = attrs[3].strip()
                    if val:
                        total += float(val)
                except ValueError:
                    pass

        return total

"""
SessionAwareAnalyzer
--------------------
Session-level first-touch attribution of e-commerce revenue to external
search engine keywords from Adobe Analytics hit-level TSV data.

Session rules:
    - Session key   : ip + user_agent
    - Session break : inactivity gap > 30 minutes between consecutive hits
    - Re-entry break: a new external search referrer within an active session
                      resets the first-touch (user came back via a new search)
    - First-touch   : the first search engine referrer seen in a session is
                      locked in and attributed to all purchases in that session

Why need session-aware?
    The search keyword attribution is session-level, not row-level. 
    The originating search engine referrer lives on the session entry hit, not the purchase hit. 
    Row-level attribution produces zero results on this dataset.
"""

import csv
import io
import logging
from datetime import datetime
from itertools import groupby
from typing import Optional

from .url_parser import is_external_search_referrer, parse_domain, parse_keyword

logger = logging.getLogger(__name__)

SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes — industry standard (GA, Adobe)


class SessionAwareAnalyzer:
    """
    Parses Adobe Analytics hit-level TSV data and answers:
        'Which external search engine keywords are driving the most revenue?'

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
    # Public API
    # ------------------------------------------------------------------

    def process(self, tsv_content: str) -> list[dict]:
        """
        Parse a full TSV string and return aggregated revenue records.

        Args:
            tsv_content: Raw tab-separated content of the hit-level data file.

        Returns:
            List of dicts with keys: Search Engine Domain, Search Keyword,
            Revenue — sorted by Revenue descending.
        """
        rows = self._parse_tsv(tsv_content)
        attributed = self._attribute_sessions(rows)
        return self._aggregate_and_sort(attributed)

    def to_tab_delimited(
        self,
        results: list[dict],
        execution_date: Optional[datetime] = None,
    ) -> tuple[str, str]:
        """
        Serialize results to a tab-delimited string and generate the output
        filename: YYYY-mm-dd_SearchKeywordPerformance.tab

        Args:
            results:        Aggregated, sorted list of result dicts.
            execution_date: Date to embed in the filename. Defaults to today (UTC).

        Returns:
            Tuple of (filename, tab_delimited_content_string).
        """
        date = execution_date or datetime.utcnow()
        filename = f"{date.strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=self.OUTPUT_COLUMNS,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in results:
            writer.writerow({
                "Search Engine Domain": row["Search Engine Domain"],
                "Search Keyword": row["Search Keyword"],
                "Revenue": f"{row['Revenue']:.2f}",
            })

        return filename, output.getvalue()

    # ------------------------------------------------------------------
    # Private — session attribution
    # ------------------------------------------------------------------

    def _attribute_sessions(self, rows: list[dict]) -> list[dict]:
        """
        Core session logic.

        1. Sort all hits by (ip, user_agent, hit_time_gmt).
        2. Group by visitor key (ip + user_agent).
        3. For each visitor, walk hits in time order:
             - Detect session breaks (timeout or re-entry via new search).
             - Lock in first-touch search referrer when first seen.
             - Emit a revenue record when a purchase hit has an attributed referrer.

        Returns:
            List of unaggregated attribution records
            {Search Engine Domain, Search Keyword, Revenue}.
        """
        sorted_rows = sorted(
            rows,
            key=lambda r: (
                r.get("ip", ""),
                r.get("user_agent", ""),
                int(r.get("hit_time_gmt", 0) or 0),
            ),
        )

        results = []
        visitor_key = lambda r: (r.get("ip", ""), r.get("user_agent", ""))

        for _, hits in groupby(sorted_rows, key=visitor_key):
            session_referrer: Optional[tuple[str, str]] = None
            last_hit_time: Optional[int] = None

            for hit in hits:
                hit_time = int(hit.get("hit_time_gmt", 0) or 0)
                referrer = hit.get("referrer", "")

                is_search_entry = is_external_search_referrer(
                    referrer, self.domains, self.site_domain
                )

                # --- Session break detection ---
                timeout_break = (
                    last_hit_time is not None
                    and (hit_time - last_hit_time) > SESSION_TIMEOUT_SECONDS
                )
                # Re-entry: user came back via a new search within the session window.
                # Previous first-touch no longer applies.
                reentry_break = is_search_entry and session_referrer is not None

                if timeout_break or reentry_break:
                    session_referrer = None

                # --- First-touch: lock in on first search entry ---
                if session_referrer is None and is_search_entry:
                    domain = parse_domain(referrer, self.domains)
                    keyword = parse_keyword(referrer, self.keyword_params)
                    if domain and keyword:
                        session_referrer = (domain, keyword)

                # --- Emit on purchase with an attributed referrer ---
                if self._is_purchase(hit.get("event_list", "")) and session_referrer:
                    revenue = self._extract_revenue(hit.get("product_list", ""))
                    if revenue > 0:
                        results.append({
                            "Search Engine Domain": session_referrer[0],
                            "Search Keyword": session_referrer[1],
                            "Revenue": revenue,
                        })

                last_hit_time = hit_time

        return results

    # ------------------------------------------------------------------
    # Private — aggregation
    # ------------------------------------------------------------------

    def _aggregate_and_sort(self, attributed: list[dict]) -> list[dict]:
        """Aggregate revenue by (domain, keyword) and sort descending."""
        totals: dict[tuple, float] = {}
        for r in attributed:
            key = (r["Search Engine Domain"], r["Search Keyword"])
            totals[key] = totals.get(key, 0.0) + r["Revenue"]

        return sorted(
            [
                {
                    "Search Engine Domain": domain,
                    "Search Keyword": keyword,
                    "Revenue": revenue,
                }
                for (domain, keyword), revenue in totals.items()
            ],
            key=lambda r: r["Revenue"],
            reverse=True,
        )

    # ------------------------------------------------------------------
    # Private — row-level helpers
    # ------------------------------------------------------------------

    def _parse_tsv(self, content: str) -> list[dict]:
        """Parse TSV content into a list of row dicts."""
        return list(csv.DictReader(io.StringIO(content), delimiter="\t"))

    def _is_purchase(self, event_list: str) -> bool:
        """Return True if event_list contains the purchase event code '1'."""
        if not event_list:
            return False
        return "1" in [e.strip() for e in event_list.split(",")]

    def _extract_revenue(self, product_list: str) -> float:
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
                    logger.warning("Could not parse revenue from product: %s", product)

        return total

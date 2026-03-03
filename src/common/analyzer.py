"""
SessionAwareAnalyzer
--------------------
Session-level first-touch attribution of e-commerce revenue to external
search engine keywords from Adobe Analytics hit-level TSV data.

Uses DuckDB as the SQL engine — the same attribution SQL runs on both
DuckDB (Lambda/CLI) and Spark SQL (EMR) with minimal dialect changes.

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

import csv
import io
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

from .url_parser import parse_domain, parse_keyword

logger = logging.getLogger(__name__)

_SQL_PATH = Path(__file__).parent / "sql" / "attribution.sql"


class SessionAwareAnalyzer:
    """
    Parses Adobe Analytics hit-level TSV data using DuckDB SQL.

    The attribution logic lives in a shared SQL file (sql/attribution.sql)
    that is portable between DuckDB and Spark SQL.
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
        self._sql = _SQL_PATH.read_text()

    # ------------------------------------------------------------------
    # Public Methods

    def process(self, tsv_content: str) -> list[dict]:
        """
        Parse a full TSV string and return aggregated revenue records.

        Pipeline:
            1. Parse TSV and enrich each row with pre-computed columns
               (_domain, _keyword, _is_purchase, _revenue) using Python.
            2. Load enriched data into DuckDB in-memory table.
            3. Execute the shared attribution SQL (pure SQL, no UDFs).

        Args:
            tsv_content: Raw tab-separated content of the hit-level data file.

        Returns:
            List of dicts with keys: Search Engine Domain, Search Keyword,
            Revenue — sorted by Revenue descending.
        """
        enriched_tsv = self._enrich(tsv_content)

        # Fast path: header-only input has no data rows to process
        line_count = enriched_tsv.count("\n")
        if line_count <= 1:
            return []

        con = duckdb.connect(":memory:")
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".tsv", delete=False
            ) as tmp:
                tmp.write(enriched_tsv)
                tmp_path = tmp.name

            con.execute(
                "CREATE TABLE hits AS SELECT * FROM read_csv_auto(?, "
                "delim='\\t', header=true, all_varchar=false)",
                [tmp_path],
            )
            Path(tmp_path).unlink(missing_ok=True)

            result = con.execute(self._sql).fetchall()
            return [
                {
                    "Search Engine Domain": row[0],
                    "Search Keyword": row[1],
                    "Revenue": float(row[2]),
                }
                for row in result
            ]
        finally:
            con.close()

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

        header = "\t".join(self.OUTPUT_COLUMNS) + "\n"
        if not results:
            return filename, header

        lines = [header]
        for r in results:
            lines.append(
                f"{r['Search Engine Domain']}\t"
                f"{r['Search Keyword']}\t"
                f"{r['Revenue']:.2f}\n"
            )
        return filename, "".join(lines)

    # ------------------------------------------------------------------
    # Pre-processing: enrich raw TSV with computed columns

    def _enrich(self, tsv_content: str) -> str:
        """
        Parse the raw TSV and append four computed columns:
            _domain      : search engine domain from referrer (or empty)
            _keyword     : search keyword from referrer (or empty)
            _is_purchase : true/false
            _revenue     : extracted revenue as float

        Returns a new TSV string with the enriched columns.
        """
        reader = csv.DictReader(io.StringIO(tsv_content), delimiter="\t")
        if not reader.fieldnames:
            return tsv_content

        out_fields = list(reader.fieldnames) + [
            "_domain", "_keyword", "_is_purchase", "_revenue",
        ]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=out_fields, delimiter="\t",
                                lineterminator="\n")
        writer.writeheader()

        for row in reader:
            referrer = row.get("referrer", "")
            row["_domain"] = parse_domain(referrer, self.domains) or ""
            row["_keyword"] = parse_keyword(referrer, self.keyword_params) or ""
            row["_is_purchase"] = str(self._is_purchase(row.get("event_list", "")))
            row["_revenue"] = str(self._extract_revenue(row.get("product_list", "")))
            writer.writerow(row)

        return output.getvalue()

    # ------------------------------------------------------------------
    # Helper functions (pure Python, also usable as Spark UDFs)

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

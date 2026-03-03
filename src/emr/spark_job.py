"""
Spark EMR job for search keyword revenue attribution.

Runs the same attribution logic as Lambda/CLI but on Spark SQL for
files > 3 GB. Designed for EMR Serverless.

Usage (EMR Serverless):
    spark-submit spark_job.py s3://input-bucket/data.tsv s3://output-bucket/

Usage (local Spark for testing):
    spark-submit spark_job.py local_input.tsv ./output/

The attribution SQL (sql/attribution.sql) is shared with the DuckDB
Lambda/CLI tier — same query, same results.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, DoubleType, StringType

# The shared SQL lives alongside common/
_SQL_PATH = Path(__file__).parent.parent / "common" / "sql" / "attribution.sql"

# Default config values (same as search_engines.toml)
SEARCH_ENGINE_DOMAINS = ["google", "bing", "yahoo", "duckduckgo", "msn", "ask", "aol"]
KEYWORD_PARAMS = ["q", "p", "query", "search"]


def main():
    if len(sys.argv) < 3:
        print("Usage: spark_job.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("SearchKeywordAttribution").getOrCreate()

    try:
        # --- Phase 1: Load and enrich with Python UDFs ---
        df = spark.read.csv(input_path, sep="\t", header=True, inferSchema=False)

        # Register URL parser UDFs
        # Import here to allow packaging url_parser.py alongside this script
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from common.url_parser import parse_domain, parse_keyword

        domains = SEARCH_ENGINE_DOMAINS
        kw_params = KEYWORD_PARAMS

        parse_domain_udf = udf(lambda ref: parse_domain(ref, domains) or "", StringType())
        parse_keyword_udf = udf(lambda ref: parse_keyword(ref, kw_params) or "", StringType())
        is_purchase_udf = udf(_is_purchase, BooleanType())
        extract_revenue_udf = udf(_extract_revenue, DoubleType())

        enriched = (
            df.withColumn("_domain", parse_domain_udf(col("referrer")))
            .withColumn("_keyword", parse_keyword_udf(col("referrer")))
            .withColumn("_is_purchase", is_purchase_udf(col("event_list")))
            .withColumn("_revenue", extract_revenue_udf(col("product_list")))
        )

        # --- Phase 2: Run shared attribution SQL ---
        enriched.createOrReplaceTempView("hits")

        attribution_sql = _SQL_PATH.read_text()
        result = spark.sql(attribution_sql)

        # --- Phase 3: Write output ---
        result.coalesce(1).write.csv(
            output_path, sep="\t", header=True, mode="overwrite"
        )

        print(f"Output written to: {output_path}")
        result.show(truncate=False)
    finally:
        spark.stop()


def _is_purchase(event_list: str) -> bool:
    """Return True if event_list contains the purchase event code '1'."""
    if not event_list:
        return False
    return "1" in [e.strip() for e in event_list.split(",")]


def _extract_revenue(product_list: str) -> float:
    """Sum revenue across all products in a product_list string."""
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


if __name__ == "__main__":
    main()

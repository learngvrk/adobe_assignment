# High-Level Design: Search Keyword Revenue Attribution

**Project:** Adobe Analytics — Search Keyword Performance Attribution
**Author:** Ranjith Gonugunta
**Date:** March 2026

---

## 1. Business Problem

> *"How much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?"*

The client operates an e-commerce site (esshopzilla.com) and collects Adobe Analytics hit-level data — every page view, cart action, and purchase is recorded as a row in a tab-separated file. The business needs to connect **marketing spend on search engine keywords** to **actual revenue generated**, so they can measure ROI and optimize their paid/organic search strategy.

### What We Need to Produce

A tab-delimited file named `YYYY-mm-dd_SearchKeywordPerformance.tab` with three columns:

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | ipod | 480.00 |
| bing.com | zune | 250.00 |

Sorted by revenue descending, so the client can immediately see which keywords drive the most revenue.

---

## 2. Understanding the Data

The input is Adobe Analytics **hit-level data** — each row represents a single HTTP request (a "hit") from a visitor on the client's site. The sample dataset contains **22 hits** from **4 unique visitors** across **12 columns**.

### Key Columns for Attribution

| Column | Role in Attribution |
|---|---|
| `hit_time_gmt` | Unix timestamp — used to order hits and detect session timeouts |
| `ip` | Visitor identifier (combined with user_agent) |
| `user_agent` | Browser fingerprint — disambiguates visitors sharing an IP |
| `referrer` | Where the visitor came FROM — this is where search engine URLs appear |
| `page_url` | Where the visitor IS — always the client's own site |
| `event_list` | Comma-separated event codes; `1` = purchase |
| `product_list` | Semicolon-delimited product attributes; revenue is the 4th field |

### Critical Distinction: `referrer` vs `page_url`

- **`referrer`**: the URL of the previous page. On the session entry hit, this is the external search engine URL (e.g., `http://www.google.com/search?q=Ipod`). On subsequent internal pages, this becomes an internal URL (e.g., `http://www.esshopzilla.com/cart/`).
- **`page_url`**: always points to the client's site. Never contains search engine data.

This distinction is the reason why **row-level attribution fails** — see Section 3.

---

## 3. The Data Story: Tracing All 4 Visitors

### Visitor 1: IP `67.98.123.1` (Google / "ipod")

| Time | Page | Referrer | Event |
|---|---|---|---|
| 06:34 | Home | **google.com/search?q=Ipod** | — |
| 06:41 | Search Results | esshopzilla.com | — |
| 06:46 | Ipod Nano 8GB | esshopzilla.com/search | Product View |
| 06:51 | Search Results | esshopzilla.com/product | — |
| 06:56 | Ipod Touch 32GB | esshopzilla.com/search | Product View |
| 07:01 | Shopping Cart | esshopzilla.com/product | Cart Add |
| 07:04 | Checkout | esshopzilla.com/cart | Checkout |
| 07:06 | Confirmation | esshopzilla.com/checkout | — |
| 07:07 | **Order Complete** | esshopzilla.com/checkout | **Purchase: $290** |

**Attribution**: Google / "ipod" / $290

### Visitor 2: IP `23.8.61.21` (Bing / "zune")

| Time | Page | Referrer | Event |
|---|---|---|---|
| 06:36 | Zune 32GB | **bing.com/search?q=Zune** | Product View |
| 06:42 | Shopping Cart | esshopzilla.com/product | Cart Add |
| 06:47 | Checkout | esshopzilla.com/cart | Checkout |
| 06:52 | Confirmation | esshopzilla.com/checkout | — |
| 06:57 | **Order Complete** | esshopzilla.com/checkout | **Purchase: $250** |

**Attribution**: Bing / "zune" / $250

### Visitor 3: IP `112.33.98.231` (Yahoo / "cd player")

| Time | Page | Referrer | Event |
|---|---|---|---|
| 06:37 | Home | **search.yahoo.com/search?p=cd+player** | — |

This visitor landed from Yahoo but **never made a purchase**. Correctly excluded from output.

### Visitor 4: IP `44.12.96.2` (Google / "ipod")

| Time | Page | Referrer | Event |
|---|---|---|---|
| 06:39 | Hot Buys | **google.com/search?q=ipod** | — |
| 06:44 | Ipod Nano 8GB | esshopzilla.com/hotbuys | Product View |
| 06:49 | Shopping Cart | esshopzilla.com/product | Cart Add |
| 06:54 | Checkout | esshopzilla.com/cart | Checkout |
| 06:59 | Confirmation | esshopzilla.com/checkout | — |
| 07:02 | **Order Complete** | esshopzilla.com/checkout | **Purchase: $190** |

**Attribution**: Google / "ipod" / $190

### Aggregated Result

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | ipod | 480.00 (290 + 190) |
| bing.com | zune | 250.00 |

---

## 4. Why Row-Level Attribution Fails

The naive approach would be: for each purchase row, look at the `referrer` column and extract the search engine domain and keyword.

**This produces zero results.** Here's why:

Looking at every purchase row (event_list = "1") in the dataset:

| IP | Purchase Referrer |
|---|---|
| 23.8.61.21 | `https://www.esshopzilla.com/checkout/?a=confirm` |
| 44.12.96.2 | `https://www.esshopzilla.com/checkout/?a=confirm` |
| 67.98.123.1 | `https://www.esshopzilla.com/checkout/?a=confirm` |

Every single purchase row has an **internal checkout referrer** — because by the time the visitor completes the purchase, they've navigated through multiple internal pages (product → cart → checkout → confirm → complete). The original Google/Bing/Yahoo referrer only appears on the **first hit** of the session.

**Conclusion**: Attribution must be session-level, not row-level. We need to carry the search engine referrer from the session entry hit forward to the purchase hit.

---

## 5. Solution: Session-Aware First-Touch Attribution

### Session Definition

| Concept | Implementation |
|---|---|
| **Session key** | `ip` + `user_agent` (identifies a unique visitor) |
| **Session break** | Inactivity gap > 30 minutes between consecutive hits |
| **First-touch** | The first external search engine referrer seen in a session is locked in |
| **Attribution** | All purchases within a session are attributed to the first-touch keyword |

### Why 30 Minutes?

The 30-minute inactivity timeout is the **industry standard** used by both Google Analytics and Adobe Analytics to define session boundaries. In this dataset, the maximum inter-hit gap for any visitor is ~6.6 minutes, so no sessions are split — but the logic correctly handles datasets where visitors return after long breaks.

### Attribution Algorithm

```
1. Sort all hits by (ip, user_agent, hit_time_gmt)
2. Group by (ip, user_agent) to identify visitors
3. Within each visitor's hits:
   a. Compute time gap between consecutive hits
   b. If gap > 30 minutes → new session (session break)
   c. Assign a session_id via cumulative sum of break flags
4. For each session:
   a. Find the first hit with an external search engine referrer
   b. Extract domain (e.g., google.com) and keyword (e.g., ipod)
   c. This becomes the session's "first-touch" attribution
5. Find all purchase hits (event_list contains "1") with revenue > 0
6. Join purchases to their session's first-touch attribution
7. Aggregate: group by (domain, keyword), sum revenue
8. Sort by revenue descending
```

### Data Flow Diagram

```
 Raw TSV File
      │
      ▼
 ┌─────────────┐
 │  Parse TSV   │  Read tab-separated hit-level data
 └──────┬──────┘
        │
        ▼
 ┌─────────────────────┐
 │  Sort & Group Hits   │  Order by (ip, user_agent, time)
 └──────┬──────────────┘
        │
        ▼
 ┌─────────────────────┐
 │  Detect Sessions     │  30-min timeout → session breaks
 │  (cumsum of breaks)  │  Assign session_id per visitor
 └──────┬──────────────┘
        │
        ├──────────────────────┐
        ▼                      ▼
 ┌──────────────┐     ┌───────────────┐
 │ First-Touch   │     │  Purchase      │
 │ Extraction    │     │  Detection     │
 │ (domain +     │     │  (event=1 +    │
 │  keyword)     │     │   revenue>0)   │
 └──────┬───────┘     └───────┬───────┘
        │                      │
        └──────────┬───────────┘
                   ▼
          ┌────────────────┐
          │  Join on        │  Attribute purchases to
          │  session key    │  their first-touch keyword
          └────────┬───────┘
                   │
                   ▼
          ┌────────────────┐
          │  Aggregate &    │  Group by (domain, keyword)
          │  Sort           │  Sum revenue, sort desc
          └────────┬───────┘
                   │
                   ▼
          ┌────────────────┐
          │  Output .tab    │  YYYY-mm-dd_SearchKeyword
          │  file           │  Performance.tab
          └────────────────┘
```

---

## 6. Architecture & Design Decisions

### Decision Log

| # | Decision | Chosen | Alternatives Considered | Rationale |
|---|---|---|---|---|
| 1 | **Attribution model** | Session first-touch | Row-level, last-touch | Row-level produces zero results on this data. First-touch matches Adobe Analytics default attribution model. |
| 2 | **SQL engine** | DuckDB | Polars, pandas, pure Python stdlib | DuckDB's SQL is portable across tiers — the same `attribution.sql` runs on DuckDB (Lambda/CLI) and Spark SQL (EMR) with zero dialect changes. Polars API doesn't translate to PySpark, requiring two separate codebases. Both DuckDB (~40MB) and Polars (~46MB) fit within Lambda's 250MB limit. |
| 3 | **Config format** | TOML + `tomllib` | YAML + PyYAML, JSON, hardcoded dict | `tomllib` is Python 3.11+ stdlib — zero external dependencies. TOML is human-readable and supports comments. YAML requires PyYAML (extra dep). |
| 4 | **Session timeout** | 30 minutes | 15 min, 60 min, `visit_num` column | Industry standard matching both Google Analytics and Adobe Analytics defaults. `visit_num` was not available in this dataset. |
| 5 | **Infrastructure** | Terraform | CloudFormation, SAM, CDK | Cloud-agnostic HCL, modular structure, readable plans. SAM is AWS-specific and YAML-heavy. |
| 6 | **Deployment model** | Lambda + S3 trigger | ECS Fargate, Step Functions | Simplest serverless pattern for event-driven file processing. Zero idle cost. Auto-scales. |
| 7 | **S3 layout** | Two separate buckets | Single bucket with prefixes | Least-privilege IAM (Lambda reads input only, writes output only). Prevents accidental trigger loops. |
| 8 | **URL parser design** | Pure stateless functions | Class with methods, regex patterns | Stateless functions register directly as Spark UDFs with zero modification. Regex is fragile for URL parsing — `urllib.parse` handles edge cases. |
| 9 | **Test framework** | pytest | unittest | pytest: less boilerplate, better fixtures, cleaner assertions, industry standard. |

---

## 7. Project Structure

```
adobe_assignment/
├── src/
│   ├── main.py                        # CLI entry point — accepts file as argument
│   ├── common/                        # Shared business logic (used by CLI, Lambda, Spark)
│   │   ├── __init__.py
│   │   ├── search_engines.toml        # Externalized config — no code deploy for changes
│   │   ├── config.py                  # TOML loader (stdlib tomllib)
│   │   ├── url_parser.py              # Pure functions: parse_domain, parse_keyword
│   │   ├── analyzer.py               # SessionAwareAnalyzer class (DuckDB engine)
│   │   └── sql/
│   │       └── attribution.sql        # Shared SQL — runs on DuckDB + Spark SQL
│   ├── lambda/                        # AWS Lambda entry point
│   │   ├── __init__.py
│   │   └── handler.py                 # S3-triggered handler
│   └── emr/                           # Spark EMR entry point
│       └── spark_job.py               # PySpark job — same SQL, distributed
├── tests/                             # 57 pytest tests
│   ├── conftest.py                    # Shared fixtures + TSV builder
│   ├── test_url_parser.py             # URL parsing edge cases
│   ├── test_config.py                 # Config loading
│   ├── test_analyzer.py               # Session attribution + integration
│   ├── test_handler.py                # Lambda handler (mocked S3)
│   ├── test_main.py                   # CLI entry point
│   └── verify_sample.py              # Manual smoke test
├── terraform/                         # Infrastructure-as-Code
│   ├── main.tf                        # Root module — wires S3 + Lambda
│   ├── variables.tf                   # Input variables
│   ├── outputs.tf                     # Output values
│   ├── terraform.tfvars               # Environment-specific values
│   └── modules/
│       ├── s3/main.tf                 # Input + output buckets
│       └── lambda/main.tf             # Function, IAM role, S3 trigger
├── scripts/
│   └── package_lambda.sh             # Builds Lambda deployment zip
├── .github/
│   └── workflows/ci.yml              # CI pipeline — pytest + smoke test
├── requirements/
│   ├── data[98].sql                   # Sample hit-level TSV data
│   └── Data-Engineer_Applicant_Programming_Exercise[95].pdf
└── docs/
    └── HLD.md                         # This document
```

---

## 8. Component Deep-Dive

### 8.1 `search_engines.toml` — Configuration Source of Truth

```toml
site_domain = "esshopzilla.com"
search_engine_domains = ["google", "bing", "yahoo", "duckduckgo", "msn", "ask", "aol"]
keyword_params = ["q", "p", "query", "search"]
```

**Purpose**: Externalize all domain-specific knowledge so adding a new search engine or keyword parameter requires a config change, not a code deployment.

**Design choice**: TOML over YAML because `tomllib` is Python 3.11+ standard library — zero external dependencies.

### 8.2 `config.py` — Config Loader

```python
def load_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict:
```

Single-function module. Default path is the co-located TOML file. Path override enables test injection. Uses `tomllib.load()` with binary mode (`"rb"`).

### 8.3 `url_parser.py` — Shared URL Parsing Functions

Three public functions, all pure and stateless:

| Function | Input | Output | Example |
|---|---|---|---|
| `is_external_search_referrer` | referrer URL, domains, site_domain | bool | `google.com/search?q=ipod` → True |
| `parse_domain` | referrer URL, domains | str or None | `search.yahoo.com/...` → `yahoo.com` |
| `parse_keyword` | referrer URL, keyword_params | str or None | `?q=Ipod` → `ipod` |

**Design choice**: Pure functions (not methods on a class) so they can be registered directly as **PySpark UDFs** without modification:

```python
spark.udf.register("parse_domain", parse_domain)
spark.udf.register("parse_keyword", parse_keyword)
```

All parsing uses `urllib.parse` (stdlib) — no regex. This handles URL encoding, query string escaping, and edge cases that regex-based parsers miss.

### 8.4 `analyzer.py` — SessionAwareAnalyzer

The core business logic class. Uses **DuckDB** as the SQL engine with a **shared SQL query** (`sql/attribution.sql`) that is portable between DuckDB (Lambda/CLI) and Spark SQL (EMR).

**Public API:**
- `process(tsv_content: str) -> list[dict]` — end-to-end processing
- `to_tab_delimited(results, execution_date) -> (filename, content)` — output serialization

**Pipeline (two phases):**

**Phase 1 — Python enrichment** (`_enrich`): Pre-computes four columns on each row using Python:

| Column | Source | Purpose |
|---|---|---|
| `_domain` | `parse_domain(referrer)` | Normalized search engine domain |
| `_keyword` | `parse_keyword(referrer)` | Extracted and normalized search keyword |
| `_is_purchase` | `_is_purchase(event_list)` | Boolean purchase flag |
| `_revenue` | `_extract_revenue(product_list)` | Summed revenue as float |

**Phase 2 — DuckDB SQL** (`attribution.sql`): Pure SQL operating on the enriched columns:

| CTE | SQL Operation | Purpose |
|---|---|---|
| `gaps` | `LAG() OVER (PARTITION BY ip, user_agent)` | Compute inactivity gaps |
| `sessions` | `SUM(CASE gap > 1800) OVER (...)` | Assign session IDs via cumulative sum |
| `first_touch` | `FIRST(_domain), FIRST(_keyword) GROUP BY session` | First-touch per session |
| `purchases` | `WHERE _is_purchase` | Identify purchase rows |
| Final | `JOIN purchases ON session → GROUP BY → ORDER BY` | Aggregate and sort |

**Why two phases?** DuckDB's Python UDF registration requires numpy. By pre-computing in Python and using pure SQL for attribution, we eliminate the numpy dependency (~25MB) and keep the SQL truly portable — the same `attribution.sql` runs on both DuckDB and Spark SQL without modification.

**Static helpers:**
- `_is_purchase(event_list)` — checks for event code "1" (exact match, not substring)
- `_extract_revenue(product_list)` — sums revenue across comma-delimited products, 4th semicolon field

### 8.5 `main.py` — CLI Entry Point

```bash
python src/main.py <input_tsv_file>
```

Satisfies requirement #3: "accept a single argument, which is the file that needs to be processed." Reads the TSV, runs `SessionAwareAnalyzer.process()`, writes `.tab` output to current directory.

### 8.6 `handler.py` — Lambda Entry Point

S3-triggered Lambda function. When a `.tsv` file is uploaded to the input bucket:

1. Reads the file from S3
2. Runs `SessionAwareAnalyzer.process()`
3. Writes `.tab` output to the output bucket

Config and analyzer are initialized **once at cold start** and reused across warm invocations — minimizing latency for subsequent triggers.

---

## 9. AWS Infrastructure

### Three-Tier Architecture

The system is designed with two processing tiers, selected based on file size, plus a CLI tier for development and assessment requirements:

```
                           ┌─────────────────────┐
                           │    S3 Input Bucket    │
                           │    (*.tsv upload)     │
                           └───────────┬───────────┘
                                       │
                      ┌────────────────┼────────────────┐
                      │                │                 │
                 File < 3 GB      File > 3 GB        CLI / Dev
                      │                │                 │
                      ▼                ▼                 ▼
          ┌───────────────────┐ ┌──────────────┐ ┌──────────────┐
          │  Lambda + DuckDB  │ │  Spark EMR   │ │   CLI +      │
          │  (Python 3.12)    │ │  Serverless  │ │   DuckDB     │
          │                   │ │              │ │              │
          │  Same SQL query   │ │  Same SQL    │ │  Same SQL    │
          │  (attribution.sql)│ │  query       │ │  query       │
          └─────────┬─────────┘ └──────┬───────┘ └──────┬───────┘
                    │                  │                 │
                    ▼                  ▼                 ▼
          ┌───────────────────┐ ┌──────────────┐ ┌──────────────┐
          │  S3 Output Bucket │ │  S3 Output   │ │  Local .tab  │
          │  (*.tab results)  │ │  Bucket      │ │  file        │
          └───────────────────┘ └──────────────┘ └──────────────┘
```

### Why Three Tiers?

| Tier | Engine | File Size | Use Case | Cost Model |
|---|---|---|---|---|
| **Lambda + DuckDB** | DuckDB in-memory | < 3 GB | Real-time event-driven processing | Pay-per-invocation, zero idle |
| **Spark EMR Serverless** | Spark SQL | 3 GB — 100+ GB | Production at scale (Adobe processes hundreds of clients) | Pay-per-second, auto-scales |
| **CLI + DuckDB** | DuckDB in-memory | Local files | Development, testing, assessment requirement #3 | Free |

**Key design principle**: All three tiers share the same `attribution.sql` query and `url_parser.py` functions. The SQL is portable — DuckDB SQL and Spark SQL both support the same window functions and CTEs used in the attribution logic.

### Lambda Architecture Detail

```
                    ┌──────────────────┐
                    │  S3 Input Bucket  │
                    │  (*.tsv upload)   │
                    └────────┬─────────┘
                             │
                    S3 Event Notification
                    (ObjectCreated, suffix=.tsv)
                             │
                             ▼
                    ┌──────────────────┐
                    │  Lambda Function  │
                    │  (Python 3.12)    │
                    │  DuckDB engine    │
                    │                   │
                    │  SessionAware     │
                    │  Analyzer         │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  S3 Output Bucket │
                    │  (*.tab results)  │
                    └──────────────────┘
```

### Terraform Modules

| Module | Resources | Purpose |
|---|---|---|
| `modules/s3` | 2 S3 buckets + public access blocks | Input/output data storage |
| `modules/lambda` | Lambda function, IAM role, S3 trigger, CloudWatch logs | Compute + permissions |

### IAM Policy (Least Privilege)

The Lambda execution role has exactly three permissions:

| Permission | Scope | Why |
|---|---|---|
| `s3:GetObject` | Input bucket only | Read TSV files |
| `s3:PutObject` | Output bucket only | Write .tab results |
| `logs:*` | CloudWatch log group | Operational monitoring |

No `s3:*`, no `*` resources — least privilege.

---

## 10. Requirements Traceability

| # | Requirement | Status | Implementation |
|---|---|---|---|
| 1 | Python application deployed and executed within AWS | Done | Lambda function deployed via Terraform, triggered by S3 upload |
| 2 | Application contains at least one class | Done | `SessionAwareAnalyzer` in `src/common/analyzer.py` |
| 3 | Application accepts a single argument (file to process) | Done | `src/main.py` reads `sys.argv[1]` |
| 4 | Tab-delimited output with Search Engine Domain, Search Keyword, Revenue | Done | `to_tab_delimited()` produces exact columns |
| 5 | Header row included | Done | `OUTPUT_COLUMNS` constant |
| 6 | Sorted by revenue descending | Done | `ORDER BY "Revenue" DESC` in attribution SQL |
| 7 | Filename: `YYYY-mm-dd_SearchKeywordPerformance.tab` | Done | Date-formatted in `to_tab_delimited()` |
| **Bonus** | Unit test cases | Done | 57 pytest tests across 6 test files |
| **Bonus** | Serverless deployment scripts | Done | Terraform IaC + `package_lambda.sh` |
| **Bonus** | Business problem presentation | Done | This HLD document |

---

## 11. Testing Strategy

### Test Coverage: 57 Tests

| Test File | Tests | What It Covers |
|---|---|---|
| `test_url_parser.py` | 16 | Domain parsing, keyword extraction, edge cases (empty, internal, unknown engines) |
| `test_config.py` | 4 | Default/custom TOML loading, missing file error |
| `test_analyzer.py` | 20 | Purchase detection, revenue extraction, session timeout, multi-session attribution, sample data integration |
| `test_handler.py` | 3 | Lambda handler with mocked S3 (read, write, fallback bucket) |
| `test_main.py` | 4 | CLI argument handling, output content, error cases |
| `verify_sample.py` | — | Manual smoke test against sample data |

### CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and PR:

- **Matrix**: Python 3.12 + 3.13
- **Steps**: Install dependencies → Run 57 tests → CLI smoke test against sample data

---

## 12. Scalability Analysis

The assessment notes: *"Our team deals with extremely large files, over 10 GB per file uncompressed."*

### Tier 1: Lambda + DuckDB (< 3 GB)

| Dimension | Capability |
|---|---|
| **File size** | Up to ~3 GB (Lambda allows 10 GB memory; DuckDB needs ~2-3x file size in RAM) |
| **Concurrency** | Auto-scales — each file upload triggers an independent Lambda invocation |
| **Cost** | Pay-per-invocation — zero idle cost |
| **Cold start** | ~1-2 seconds (DuckDB is lightweight) |
| **Timeout** | 15 minutes max — sufficient for 3 GB files |
| **Temp storage** | 10 GB `/tmp` — used for DuckDB temp files |

**Why Lambda caps at ~3 GB, not 10 GB**: DuckDB's in-memory SQL engine needs ~2-3x the file size for sorting, window functions, and joins. A 3 GB TSV file needs ~6-9 GB RAM, fitting within Lambda's 10 GB limit. A 10 GB file would require ~20-30 GB RAM — exceeding Lambda.

### Tier 2: Spark EMR Serverless (3 GB — 100+ GB)

For files exceeding Lambda's capacity, the architecture scales to **EMR Serverless** running Spark SQL:

| Aspect | Lambda + DuckDB | Spark EMR Serverless |
|---|---|---|
| **File size** | < 3 GB | 3 GB — 100+ GB (distributed) |
| **Processing** | Single-node, in-memory | Multi-node, partitioned |
| **SQL query** | Same `attribution.sql` | Same SQL via Spark SQL |
| **URL parsers** | Same `url_parser.py` | Same functions as Spark UDFs |
| **Config** | Same `search_engines.toml` | Same TOML file |
| **Cost** | Pay-per-invocation | Pay-per-second, auto-scales |
| **Cold start** | ~1-2 seconds | 10 seconds — 3 minutes |

**Why Spark EMR for scale?** Adobe processes hit-level data for hundreds of clients at production scale. The sample data (22 rows, 1 client) is tiny — in production, a single client's data can be 10 GB+, and the platform serves hundreds of clients simultaneously. Spark's distributed processing handles this natively with data partitioning across worker nodes.

### Shared SQL: The Key to Portability

The same `attribution.sql` query runs on both DuckDB and Spark SQL because:

1. **Window functions** (`LAG`, `SUM OVER`) are standard SQL — supported by both engines
2. **CTEs** (`WITH ... AS`) are standard SQL — supported by both engines
3. **Aggregation** (`GROUP BY`, `SUM`, `FIRST`) — supported by both engines
4. **Pre-computed columns** (`_domain`, `_keyword`, `_is_purchase`, `_revenue`) eliminate engine-specific UDF registration

On **Spark**, the enrichment phase uses PySpark UDFs instead of Python csv processing:

```python
from common.url_parser import parse_domain, parse_keyword

parse_domain_udf = udf(lambda ref: parse_domain(ref, domains), StringType())
parse_keyword_udf = udf(lambda ref: parse_keyword(ref, keyword_params), StringType())

df = df.withColumn("_domain", parse_domain_udf(col("referrer")))
df = df.withColumn("_keyword", parse_keyword_udf(col("referrer")))
```

Then the same attribution SQL runs via `spark.sql(attribution_sql)`.

### Lambda Constraint Analysis

| Lambda Limit | Value | Impact on 10 GB Files |
|---|---|---|
| **Memory** | 10 GB max | Insufficient — DuckDB needs ~20-30 GB for 10 GB file |
| **Timeout** | 15 minutes | Borderline — DuckDB can process 3 GB in ~5 min |
| **Temp storage** | 10 GB `/tmp` | Matches file size but no room for DuckDB spill files |
| **CPU** | Up to 6 vCPUs at 10 GB RAM | Session attribution is single-threaded (global sort) |
| **Payload** | 6 MB sync / 256 KB async | Must read from S3, not inline |

**Threading consideration**: Python's GIL blocks CPU parallelism. `multiprocessing.Pool` fails in Lambda (no `/dev/shm`). DuckDB uses its own internal threads for SQL execution, but the session attribution requires a global sort — inherently sequential.

### Additional Scaling Strategies

| Strategy | Benefit |
|---|---|
| **DuckDB out-of-core** | DuckDB can spill to disk for datasets larger than RAM via `/tmp` |
| **Partitioned input** | Split large files by date/region before processing |
| **S3 Select** | Push filtering to S3 — only read rows with external referrers |
| **Compression** | Accept gzipped TSV — reduces I/O time significantly |

---

## 13. Next Steps

| Item | Priority | Description |
|---|---|---|
| **EMR Serverless Spark job** | High | `src/emr/spark_job.py` — reuses `attribution.sql` via Spark SQL, `url_parser.py` as UDFs, same TOML config |
| **Tier routing** | High | S3 event → Step Functions: route to Lambda (< 3 GB) or EMR (> 3 GB) based on `ContentLength` |
| **README** | Medium | Setup instructions, quickstart, architecture overview |
| **.gitignore** | Low | Exclude `build/`, `__pycache__/`, `.terraform/`, `*.tfstate` |
| **Monitoring** | Future | CloudWatch alarms for Lambda errors, EMR job metrics, S3 lifecycle policies |
| **Data validation** | Future | Schema validation on input TSV before processing |

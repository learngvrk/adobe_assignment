# Design Evolution: Search Keyword Revenue Attribution

A timeline of how the solution architecture evolved through iterative design, challenged assumptions, and evidence-based pivots.

---

## Timeline

### Phase 1: Project Scaffolding — Mar 1
`09fe6b9`

Set up the initial folder structure and analyzed the sample data. Identified 12 columns in the hit-level TSV, 22 rows from 4 unique visitors. Key insight: the `referrer` column on the first hit of a session contains the external search engine URL, while `page_url` always points to the client's site.

### Phase 2: First Implementation (Polars) — Mar 2
`df26f16`

Built the core pipeline: `url_parser.py` (pure stdlib functions), `config.py` (TOML loader), and `analyzer.py` using **Polars DataFrames** for session-aware attribution.

**Critical discovery**: Row-level attribution produces **zero results**. Every purchase row has an internal checkout referrer (`esshopzilla.com/checkout/?a=confirm`), not a search engine URL. The search engine referrer only appears on the session entry hit, 5–8 pages before the purchase.

**Pivot**: Designed session-aware first-touch attribution — carry the entry referrer forward across the entire session.

### Phase 3: Polars → Pandas → Polars — Mar 2
`0c8f2ac`

Briefly switched to pandas for wider familiarity, then realized Polars was more maintainable for the window operations needed (sort, lag, cumulative sum within partitions). Reverted to Polars.

### Phase 4: Testing & CLI — Mar 2–3
`b431a08`, `7cb4fde`

Added 53 pytest tests (later expanded to 57) covering URL parsing edge cases, config loading, session attribution logic, and Lambda handler with mocked S3. Created `main.py` CLI entry point to satisfy assessment requirement #3 ("accept a single argument").

### Phase 5: CI/CD Pipeline — Mar 3
`fadbdd9` → `357b23b`

Set up GitHub Actions on a separate branch. Hit two real-world issues:

1. **`lambda` is a Python keyword** — `from lambda.handler import handler` is a SyntaxError. Fixed with `importlib.util.spec_from_file_location`.
2. **GitHub PAT missing `workflow` scope** — push was rejected for `.github/workflows/ci.yml`. Fixed by updating the token.

These are the kind of integration issues you only find by actually deploying CI, not by running locally.

### Phase 6: Lambda Deployment + Terraform — Mar 2–3

Built Terraform modules for S3 (input/output buckets) and Lambda (function, IAM role, S3 trigger).

**Issue**: Lambda failed with "Polars binary is missing!" The `package_lambda.sh` script used `--no-deps`, which excluded the native Rust binary. Removed the flag — zip grew from **2.2 MB to 46 MB**, confirming the binary was now included. Lambda worked after redeployment.

### Phase 7: High-Level Design Document — Mar 3
`4a9f744`

Wrote a comprehensive 13-section HLD covering the business problem, data story (tracing all 4 visitors), why row-level fails, session attribution algorithm, decision log, AWS infrastructure, and scalability analysis.

### Phase 8: The DuckDB Challenge — Mar 3

After writing the HLD, questioned: **"Is DuckDB a better bet than Polars for a shared SQL layer between Lambda and Spark EMR?"**

Initially defended Polars. The argument was that DuckDB adds an "unnecessary SQL layer." But when challenged on *where exactly* the constraint was:

- **"Too heavy for serverless"** → Both Polars (46 MB) and DuckDB (~20 MB) are well within Lambda's 250 MB limit. This argument didn't hold.
- **The real differentiator**: Polars API doesn't translate to PySpark. If we need both Lambda and Spark tiers, Polars requires **two separate codebases**. DuckDB SQL is portable — the same query runs on both DuckDB and Spark SQL.

**Decision**: Switch to DuckDB.

### Phase 9: Deep Research Before Switching — Mar 3

Before committing to the switch, conducted deep technical research on three questions:

| Question | Finding |
|---|---|
| **Can Lambda handle 10 GB files?** | No. Lambda has 10 GB max memory; DuckDB needs 2–3x file size in RAM. Realistic cap: ~3 GB. |
| **Is EMR Serverless truly serverless?** | "Serverless-ish" — no cluster management, pay-per-second, but 10s–3min cold starts. |
| **Can threading scale Lambda to 10 GB?** | No. Session attribution requires a global sort (inherently sequential). Python's GIL blocks CPU parallelism. `multiprocessing.Pool` fails in Lambda (no `/dev/shm`). |

This research prevented wasted effort — without it, we might have built a threading solution that fundamentally can't work for this workload.

### Phase 10: Three-Tier Architecture — Mar 3

Designed the architecture with two processing tiers plus CLI:

| Tier | Engine | File Size | Rationale |
|---|---|---|---|
| **Lambda + DuckDB** | DuckDB in-memory | < 3 GB | Event-driven, zero idle cost |
| **Spark EMR Serverless** | Spark SQL | 3 GB – 100+ GB | Adobe serves hundreds of clients — Spark at scale |
| **CLI + DuckDB** | DuckDB in-memory | Local files | Development + assessment requirement |

Key insight from the user: *"This is just sample data for one client. Adobe serves hundreds of clients — Spark makes sense at production scale."*

### Phase 11: Polars → DuckDB Switch — Mar 3
`6c5c037`

Rewrote the analyzer with a **two-phase pipeline**:

1. **Python enrichment**: Pre-compute `_domain`, `_keyword`, `_is_purchase`, `_revenue` columns using Python's `csv` module
2. **DuckDB SQL**: Execute `attribution.sql` on the enriched data — pure SQL, no UDFs

**Why two phases?** DuckDB's `create_function()` requires numpy for Python UDF registration. Instead of adding a ~25 MB dependency, we pre-compute in Python and keep the SQL truly UDF-free and portable.

**Result**: Lambda zip dropped from **46 MB (Polars) to 20 MB (DuckDB)**. All 57 tests pass. Same output: google.com/ipod/$480, bing.com/zune/$250.

---

## Key Assertions Challenged

Throughout the project, several assumptions were tested and either validated or disproven:

### 1. "Row-level attribution works"
**Disproven.** Every purchase row has an internal checkout referrer. The search engine referrer only appears on the session entry hit. Row-level attribution produces zero results on this dataset.

### 2. "Polars is too heavy for serverless"
**Challenged.** Both Polars (46 MB) and DuckDB (20 MB) fit comfortably within Lambda's 250 MB deployment limit. The "too heavy" argument was imprecise — the real constraint is SQL portability, not package size.

### 3. "Lambda can handle 10 GB files"
**Researched and disproven.** Lambda allows 10 GB memory, but DuckDB needs 2–3x file size in RAM for sorting, window functions, and joins. A 10 GB file would need ~20–30 GB RAM. Realistic Lambda cap: ~3 GB.

### 4. "Threading can scale Lambda to 10 GB"
**Researched and disproven.** Session attribution requires a global sort across all hits per visitor — inherently sequential. Python's GIL blocks CPU parallelism. `multiprocessing.Pool` fails in Lambda's sandboxed environment (no `/dev/shm`). DuckDB uses its own internal threads, but the workload is memory-bound, not CPU-bound.

### 5. "We need a middle tier (ECS Fargate)"
**Evaluated and deferred.** ECS Fargate + DuckDB (30 GB RAM, 200 GB disk, no timeout) could handle 10 GB files. But for an Adobe-scale platform serving hundreds of clients, Spark is the natural choice — it's what Adobe already uses in production. The middle tier adds complexity without clear benefit for this use case.

---

## What This Shows

This evolution demonstrates:

- **Evidence-based decision making** — not picking a tool and defending it, but testing assumptions with data
- **Willingness to pivot** — three different DataFrame/SQL engines evaluated, switched when evidence supported it
- **Research before building** — deep-dived Lambda constraints before committing to the DuckDB switch, preventing wasted work
- **Practical architecture** — chose technologies that match the real production context (Adobe + Spark), not just the sample data

# Design Evolution

How the solution took shape — what I tried, what broke, and why I changed direction.

---

## Mar 1 — Getting started
`09fe6b9`

Set up the folder structure and dug into the sample data. 12 columns in the hit-level TSV, 22 rows, 4 unique visitors. The key thing I noticed: `referrer` on the first hit of a session has the search engine URL, but `page_url` always points to the client's site (esshopzilla.com). That distinction ended up being important later.

## Mar 2 — First attempt with Polars
`df26f16`

Built the core pipeline: `url_parser.py` for extracting domains and keywords (pure stdlib), `config.py` for TOML loading, and `analyzer.py` using Polars DataFrames.

Ran it and got... zero results. Turns out every purchase row has an internal checkout referrer (`esshopzilla.com/checkout/?a=confirm`), not a search engine URL. The search engine referrer only shows up on the session entry hit — 5 to 8 pages before the purchase. Row-level attribution simply doesn't work for this data.

So I redesigned it around session-aware first-touch attribution: grab the search engine referrer from the session entry, carry it forward through the whole session, and attribute revenue at the session level.

## Mar 2 — Quick Polars vs Pandas detour
`0c8f2ac`

Briefly switched to pandas thinking it might be simpler. Realized Polars was actually cleaner for the window operations I needed (sort, lag, cumulative sum within partitions). Switched back.

## Mar 2–3 — Tests and CLI
`b431a08`, `7cb4fde`

Wrote 53 pytest tests covering URL parsing edge cases, config loading, session attribution, and the Lambda handler with mocked S3. Built `main.py` as the CLI entry point — the assessment requires the app to "accept a single argument, which is the file."

## Mar 3 — CI/CD surprises
`fadbdd9` → `357b23b`

Set up GitHub Actions. Two things bit me:

1. `lambda` is a Python keyword — `from lambda.handler import handler` is a SyntaxError. Had to use `importlib.util.spec_from_file_location` instead.
2. My GitHub PAT was missing the `workflow` scope, so pushing `.github/workflows/ci.yml` got rejected.

Classic integration issues you only hit when you actually wire up CI.

## Mar 2–3 — Lambda + Terraform

Built Terraform modules for S3 buckets and the Lambda function (IAM role, permissions).

First deploy failed: "Polars binary is missing!" My `package_lambda.sh` used `--no-deps` which excluded the native Rust binary. Removed the flag and the zip jumped from 2.2 MB to 46 MB — confirming the binary was now in there. Lambda worked after that.

## Mar 3 — Writing the HLD
`4a9f744`

Wrote a 13-section high-level design doc. Sounds tedious but it forced me to think through the whole architecture properly — the data story, why row-level fails, the attribution algorithm, AWS infrastructure choices. Writing it down exposed gaps I hadn't thought about.

## Mar 3 — The DuckDB question

After the HLD, I asked myself: is DuckDB a better fit than Polars for sharing logic between Lambda and Spark?

My first instinct was no — "DuckDB adds an unnecessary SQL layer." But then I thought about it more carefully:

- The "too heavy for serverless" argument didn't hold up. Polars is 46 MB, DuckDB is ~20 MB. Both fit in Lambda's 250 MB limit easily.
- The real problem: Polars API doesn't translate to PySpark at all. If I want both a Lambda tier and a Spark tier, Polars means maintaining two completely separate codebases. DuckDB SQL is portable — same query runs on both DuckDB and Spark SQL.

That settled it. Switched to DuckDB.

## Mar 3 — Research before building

Before committing to the switch, I researched three things:

- **Can Lambda handle 10 GB files?** No. Lambda maxes out at 10 GB RAM, but DuckDB needs 2–3x file size for sorting and window functions. Realistic cap is ~3 GB.
- **Is EMR Serverless actually serverless?** Sort of. No cluster management, pay-per-second, but 10s–3min cold starts. "Serverless-ish."
- **Can threading scale Lambda to 10 GB?** No. Session attribution needs a global sort — inherently sequential. Python's GIL blocks CPU parallelism, and `multiprocessing.Pool` fails in Lambda (no `/dev/shm`).

This saved me from building a threading solution that fundamentally can't work for this workload.

## Mar 3 — Three-tier design

Landed on three tiers:

| Tier | Engine | File Size | Why |
|---|---|---|---|
| CLI + DuckDB | DuckDB | Local files | Dev + assessment requirement |
| Lambda + DuckDB | DuckDB | < 3 GB | On-demand, zero idle cost |
| Spark EMR | Spark SQL | 3 GB – 100+ GB | Adobe serves hundreds of clients — this is where Spark makes sense |

The same `attribution.sql` runs on all tiers without modification.

## Mar 3 — The actual switch to DuckDB
`6c5c037`

Rewrote the analyzer as a two-phase pipeline:

1. **Python enrichment** — pre-compute `_domain`, `_keyword`, `_is_purchase`, `_revenue` columns using Python's `csv` module
2. **DuckDB SQL** — execute `attribution.sql` on the enriched data

Why two phases? DuckDB's `create_function()` needs numpy for Python UDF registration. Instead of adding a ~25 MB dependency, I pre-compute in Python and keep the SQL UDF-free and portable.

Result: Lambda zip dropped from 46 MB (Polars) to 20 MB (DuckDB). All tests pass. Same output: google.com/ipod/$480, bing.com/zune/$250.

## Mar 4 — EMR Serverless deployment
`feat/ci-cd-pipeline`

Deployed to EMR Serverless and hit a bunch of issues that only surface when you actually run on the real infrastructure:

**Capacity tuning** — Spark's defaults (4 cores, 14 GB per executor, dynamic allocation requesting 3 executors) demanded ~42 GB. Way too much for a demo workload. Had to explicitly set `executor.cores=1`, `executor.memory=2g`, `executor.instances=1` and disable dynamic allocation.

**Zip files don't get extracted** — EMR Serverless keeps `--py-files` zips on `sys.path` without extracting to disk. So `Path(__file__).parent` points to a temp dir, not the zip contents. Fixed with `pkgutil.get_data()` which reads from inside zips. On EMR on EC2 with YARN, this wouldn't be an issue since YARN extracts zips to the working directory.

**SQL dialect mismatch** — The shared `attribution.sql` uses DuckDB-style double-quoted aliases (`AS "Revenue"`). Spark SQL wants backticks. Added a regex replacement at runtime instead of maintaining two SQL files.

**Missing IAM permissions** — Spark checks if the output directory exists before writing (`s3:ListBucket`) and needs `s3:DeleteObject` for overwrite mode. Both were missing initially.

**Bottom line on EMR Serverless vs EC2**: Serverless is great for this demo (zero idle cost, no cluster management). But for Adobe-scale production with hundreds of concurrent jobs, EMR on EC2 makes more sense — persistent clusters, shared resource pools, reserved pricing, and none of the zip/capacity quirks.

## Mar 4–5 — Lambda direct invocation fix

Realized the Lambda was triggered by S3 events (auto-fires on upload), but the assessment says "accept a single argument, which is the file." Changed the handler to accept `{"file": "s3://bucket/key.tsv"}` via direct `aws lambda invoke`. Removed the S3 trigger from Terraform. Added validation tests. Both Lambda and EMR tested end-to-end on AWS — output matches expected results.

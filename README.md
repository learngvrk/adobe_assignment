# Search Keyword Revenue Attribution

Revenue attribution from external search engines using Adobe Analytics hit-level data. Determines which search keywords drive the most revenue through session-aware first-touch attribution.

## Business Problem

> How much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?

The client's e-commerce site (esshopzilla.com) collects Adobe Analytics hit-level data. This pipeline connects **search engine keywords** to **actual revenue**, enabling the client to measure ROI on their search strategy.

## Architecture

Three-tier design with a shared SQL attribution query (`sql/attribution.sql`) across all tiers:

| Tier | Engine | File Size | Use Case |
|---|---|---|---|
| **CLI** | DuckDB | Local files | Development, assessment requirement |
| **Lambda** | DuckDB | < 3 GB | On-demand S3 processing, zero idle cost |
| **Spark EMR** | Spark SQL | 3 GB – 100+ GB | Production scale for large data feeds |

The same attribution SQL runs on both DuckDB and Spark SQL without modification.

AWS Glue was evaluated as an alternative to Lambda but deferred due to cold start latency (1-3min vs 1-2s) and lack of free tier — see [HLD](docs/HLD.md) for the full analysis.

## Project Structure

```
src/
  main.py                     # CLI entry point (accepts single TSV file argument)
  common/
    analyzer.py               # Core attribution engine (SessionAwareAnalyzer)
    url_parser.py             # URL/keyword parsing (pure stdlib, Spark UDF-safe)
    config.py                 # TOML config loader
    search_engines.toml       # Search engine domains + keyword param config
    sql/attribution.sql       # Shared attribution SQL (DuckDB + Spark SQL)
  lambda/
    handler.py                # AWS Lambda entry point (accepts S3 file path)
  emr/
    spark_job.py              # EMR Serverless Spark job
tests/                        # 59 pytest tests
terraform/                    # IaC for S3 buckets, Lambda, EMR Serverless, IAM roles
scripts/
  package_lambda.sh           # Lambda deployment packaging
  package_emr.sh              # EMR Spark code packaging + S3 upload
  submit_emr_job.sh           # Submit Spark job to EMR Serverless
docs/
  HLD.md                      # High-Level Design (13 sections)
  design_evolution.md         # Design timeline and decision log
```

## Quick Start (CLI)

```bash
# Install dependencies
pip install -r requirements.txt

# Run attribution on sample data
python src/main.py "requirements/data[98].sql"

# Output: YYYY-mm-dd_SearchKeywordPerformance.tab
```

Expected output for the sample data:

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | ipod | 480.00 |
| bing.com | zune | 250.00 |

## AWS Deployment (Lambda)

```bash
# 1. Package Lambda zip (~20 MB with DuckDB)
chmod +x scripts/package_lambda.sh
./scripts/package_lambda.sh

# 2. Deploy infrastructure
cd terraform
terraform init
terraform apply

# 3. Upload a TSV file to S3
aws s3 cp "requirements/data[98].sql" s3://<input-bucket>/data.tsv

# 4. Invoke Lambda with the file to process (single argument)
aws lambda invoke --function-name <function-name> \
    --payload '{"file": "s3://<input-bucket>/data.tsv"}' \
    --cli-binary-format raw-in-base64-out response.json

# Get function-name from: cd terraform && terraform output
```

The Lambda reads the file from S3, runs attribution, and writes the result to the output bucket.

## AWS Deployment (EMR Serverless — Spark)

EMR Serverless is included for the assessment demo. In production, EMR on EC2 clusters would handle Adobe-scale workloads (hundreds of concurrent jobs, reserved instance pricing).

```bash
# 1. Deploy infrastructure (included in terraform apply above)
# The EMR Serverless application is created alongside Lambda.

# 2. Package and upload Spark code to S3
chmod +x scripts/package_emr.sh
./scripts/package_emr.sh <input-bucket>

# 3. Submit a Spark job
chmod +x scripts/submit_emr_job.sh
./scripts/submit_emr_job.sh <app-id> <role-arn> <input-bucket> <output-bucket>

# Get app-id and role-arn from: cd terraform && terraform output

# 4. Check output
aws s3 ls s3://<output-bucket>/emr-output/
```

## Running Tests

```bash
pytest              # 57 tests across 6 test modules
pytest -v           # verbose output
```

Test coverage includes: URL parsing edge cases, config loading, session attribution logic, Lambda handler (mocked S3), and CLI subprocess tests.

## Scalability

- **Lambda + DuckDB** handles files up to ~3 GB (DuckDB needs 2-3x file size in RAM; Lambda max is 10 GB)
- **Spark EMR Serverless** handles 10 GB+ files with distributed processing
- Session timeout, search engine domains, and keyword params are all configurable via `search_engines.toml`

See [docs/HLD.md](docs/HLD.md) for detailed scalability analysis and architectural decisions.

## Documentation

- [High-Level Design](docs/HLD.md) — business problem, data story, attribution algorithm, AWS infrastructure, scalability
- [Design Evolution](docs/design_evolution.md) — timeline of design iterations, challenged assumptions, and evidence-based pivots

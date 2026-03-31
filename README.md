# Transaction Feature Pipeline

A PySpark pipeline that aggregates transactions for the last 7 days and create features used by AI Models.

## Features computed

| Feature | Description |
|---|---|
| `feature_1` | Count of credit wire transactions in the last 7 days |
| `feature_2` | Count of debit wire transactions in the last 7 days |
| `feature_3` | Total value of card transactions (credit + debit) in the last 7 days |

Output contains one row per `(ref_date, customer_id)` for the configured reference dates.




## Architecture

```
[Transactions.csv, TRansactionTypes.csv] -> S3 (Bronze) -> EMR Serverless / PySpark -> S3 (Silver/Iceberg) -> S3(Gold / Iceberg) -> Athena
```

- **Processing** - EMR Serverless with PySpark
- **Storage** - S3 with Bronze / Silver / Gold medallion layers
- **Table format** - Apache Iceberg
- **Metadata** - AWS Glue Data Catalog
- **Querying** - Amazon Athena
- **Warehouse** - Redshift/Snowflake
- **Feature Store** - Sagemaker
- **Orchestrator** - Airflow

An architecture diagram is also included in the repository as a png file.

## Getting started

### Prerequisites for local deployment

- Python 3.11+
- Java 11+ (required by Spark)

### Install dependencies

```bash
pip install -r requirements-dev.txt
```

### Run locally

Place `Transactions.csv` and `TransactionType.csv` in the `data/` directory, then:

```bash
python main.py
```

Output is written to `output/features_output.csv`.

### Run on EMR Serverless

Update the paths in `config.py` to point to your S3 bucket:

```python
class PathConfig(BaseModel):
    transactions: str = "s3://your-bucket/data/Transactions.csv"
    transaction_types: str = "s3://your-bucket/data/TransactionType.csv"
    output_dir: str = "s3://your-bucket/output"
```

Package and submit:

```bash
zip -r pipeline_code.zip main.py config.py utils/ pipeline/ approaches/ \
  -x "*.pyc" -x "*__pycache__*"

aws s3 cp pipeline_code.zip s3://your-bucket/code/pipeline_code.zip

aws emr-serverless start-job-run \
  --application-id <application-id> \
  --execution-role-arn arn:aws:iam::<account-id>:role/EMRServerlessExecutionRole \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/code/main.py",
      "sparkSubmitParameters": "--py-files s3://your-bucket/code/pipeline_code.zip"
    }
  }'
```

## Configuration

All configuration lives in `config.py`.

| Class | Purpose |
|---|---|
| `PathConfig` | File paths (local or S3) |
| `SchemaConfig` | Column name mappings |
| `FeatureConfig` | Transaction types, credit/debit codes, window size |
| `PipelineConfig` | Reference dates, app name, log level |

## Validation

Source data is validated before any processing using plain PySpark. Critical failures raise a ValueError and halt the pipeline. Schema drift (unexpected new columns) logs a warning and continues.

## Tests

```bash
# All tests
pytest tests/ -v

# Unit tests only (no Spark required)
pytest tests/unit -v

# Data quality tests
pytest tests/validation -v

# With coverage
pytest tests/unit -v --cov=pipeline --cov=config --cov-report=term-missing
```

## CI/CD

### CI — runs on every push and pull request

- Linting via `ruff`
- Unit tests
- Validation tests

No AWS credentials required.

### CD — runs on merge to main

Requires the following GitHub repository secrets:

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_ACCOUNT_ID` | AWS account ID |
| `S3_BUCKET` | S3 bucket name |
| `EMR_APPLICATION_ID` | EMR Serverless application ID |

Add secrets to Github repo under Secrets and Variables found in Repo Settinfs.

## Design decisions

**Broadcast join** — `TransactionType` is a small static lookup table broadcast explicitly to avoid shuffling the large transactions dataset. Spark may auto-broadcast tables under 10MB, but the intent is made explicit so it is not sensitive to configuration changes.

# Databricks Cost Connector

A Python-based connector that discovers Databricks cost-related system tables, collects available data, saves raw records, calculates list costs when matching pricing information is available, and converts usage records into a consistent format. It also generates reports showing which data sources were collected and which were unavailable or empty.

## Requirements

Before running the connector, make sure you have:

- Python 3.10 or later
- A Databricks workspace with an available SQL warehouse or supported compute endpoint
- Read permissions for the system tables you want to collect
- The dependencies listed in `requirements.txt`

The project uses these pinned dependencies:

- `databricks-sql-connector==4.6.0`
- `python-dotenv==1.2.4`

Databricks billing and pricing data may take time to appear. If a query succeeds but returns no records, the source is reported as `empty`. This does not necessarily mean the table is missing.

## Setup

The following commands work in PowerShell on Windows.

### 1. Create and activate a virtual environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install the dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 3. Configure your environment variables

Create a local `.env` file from the example:

```powershell
Copy-Item .env.example .env
```

Open `.env` and provide your Databricks connection details:

```text
DATABRICKS_SERVER_HOSTNAME=your-workspace-hostname
DATABRICKS_HTTP_PATH=your-warehouse-http-path
DATABRICKS_TOKEN=your-development-token
DATABRICKS_ACCOUNT_ID=
DATABRICKS_WORKSPACE_ID=
```

The account and workspace IDs are optional. When possible, the connector discovers them from the available data.

Keep your `.env` file private. Never commit access tokens or other credentials to Git or include them in the submission.

## Running the Connector

You can specify the collection window using ISO 8601 timestamps with a timezone. The start time is inclusive and the end time is exclusive.

Run the following command from the project directory:

```powershell
python -m connector `
  --start 2026-10-01T00:00:00Z `
  --end 2026-10-06T00:00:00Z `
  --output outputs `
  --batch-size 1000
```

You can also run the connector using `python -m connector.cli` with the same arguments.

During execution, the connector checks authentication, discovers visible system tables, and attempts to collect data from each configured source independently. It then calculates list costs where the available pricing data supports the calculation, normalizes applicable usage records, and generates coverage reports.

## Output Files

After running the connector, the output folder contains the following files:

raw/ - Raw data collected from Databricks.

normalized/ - Normalized cost records in a consistent format.

samples/ - Redacted examples of raw and normalized records.

coverage_report.csv and coverage_report.json - Summary of category coverage.

source_coverage_report.csv and source_coverage_report.json - Collection status for each source.

discovery_reconciliation.json - Details of discovered sources and their collection status.

live_execution_evidence_redacted.json - Redacted evidence from the live connector run.


The raw files preserve source fields and nested metadata. Decimal values are stored as strings to preserve their precision.

The sample files and live execution evidence are redacted to protect account and workspace identifiers, email addresses, credentials, and query contents. Full raw exports may contain sensitive information, so keep them private and do not include them in a public repository.

The coverage reports summarize the 17 assessment categories and the 27 configured data sources. The discovery reconciliation report provides additional information about visible tables that were not configured for collection and explains their status. If a table-listing operation fails, that failure is recorded separately.

A source can contribute data to more than one category, so category record counts do not necessarily add up to the total number of unique records.

## Understanding Source Statuses

Each source or category is assigned a status to explain what happened during collection.

- `collected` - Records were retrieved successfully.
- `empty` - The source was queried successfully but returned no applicable records during the selected time window.
- `permission_denied` - Access was denied or authentication failed.
- `unavailable_in_account` - The candidate schema or table was not available in the account.
- `unsupported` - The account or feature does not support the requested operation.
- `failed` - Collection encountered another error.

These distinctions help explain whether data was collected, absent, inaccessible, unsupported, or affected by an error. Optional source failures are handled independently so they do not automatically prevent other sources from being collected.

The reports also include record counts, relevant timestamps, permission requirements, and explanations for sources that could not be collected.

## Pricing and Normalization

List costs are calculated separately from normalization. To calculate a cost, the connector matches the usage record with pricing information based on the account, SKU, cloud, usage unit, and applicable time period.

The connector prefers `pricing.effective_list.default`, followed by `pricing.default` and then `pricing.promotional.default`. Calculations use decimal arithmetic to preserve precision.

If a matching price is unavailable, the list cost remains null. When a usage interval spans multiple prices and the source does not provide the quantity associated with each price period, the connector does not invent a quantity or calculate an unsupported total.

Contracted, effective, and billed costs are not inferred when the source does not provide them. Available list pricing is not treated as proof of a negotiated contract or commercial commitment.

The normalizer preserves source identifiers and metadata, uses null for missing values, and does not perform pricing calculations itself.

## Timestamp Handling

The connector explicitly sets the Databricks SQL session timezone to UTC. Datetime values returned without timezone information are consistently interpreted as UTC throughout collection, coverage reporting, pricing, normalization, and serialization.

Timezone-aware input timestamps are converted to UTC as well. This prevents results from depending on the local timezone of the workstation or SQL session.

Date-only fields such as `ingestion_date` remain in the source metadata rather than being treated as precise update timestamps.

## Automated Tests

The automated tests use synthetic records and fake cursors, so they can run without Databricks credentials.

To run the tests, execute:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

The test suite covers authentication-related behavior, collection, normalization, timestamp handling, decimal precision, pricing, coverage reporting, and command-line execution.

## Assessment Coverage

The connector investigates Databricks cost-related sources covering billing and usage, compute, SQL warehouses, Lakeflow jobs and pipelines, storage, query history, access and network information, serving, and pricing.

The 17 assessment categories cover:

- Billing and usage
- Account and workspace consumption
- Cluster and compute consumption
- SQL warehouse consumption
- Jobs and pipeline consumption
- Serverless consumption
- Storage-related usage
- Data-transfer and network-related usage
- SKU and pricing information
- List-cost calculations
- Resource and workload attribution
- User and service-principal attribution
- Tags and allocation metadata
- Budgets, alerts, and cost controls
- Corrections, retractions, and restatements
- Optimization and cost-saving information
- Contract, commitment, and discount information

Some sources may require additional permissions, account capabilities, or features. When a category cannot be collected, the reports explain the observed status rather than silently omitting it.

The redacted live execution evidence documents the authentication result, account and workspace discovery, investigated sources and categories, record counts, collection time window, and source statuses.

This connector focuses on live data access, collection, normalization, and transparent coverage reporting. It does not infer information that is absent from the available source data.
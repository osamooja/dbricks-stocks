## Setup the workspace
1. Setup Databricks Workspace in Azure portal.
2. Connect with Github.
3. Create a cluster in Databricks Workspace Compute section.

## Architecture
Each integration has it's own job to ingest data from the source to Bronze Layer (`bronze_{source_system}` schema) while adding lakehouse_ fields (`lakehouse_pk` & `lakehouse_load_ts`).\
Silver Layer is combining the tables from each source under `silver_stocks` schema with cleaning, filtering and minor aggregates.\
On Gold Layer we perform extensive aggregations and enrich the data.

## Jobs
- Jobs are managed in Databricks Workflows, each integration is configured to an own job.
- Source code for jobs is coming from Github defined by Tag, which is created on release (e.g. v1.1.0). 
- Integration configuration are found in the `project/{integration_name}/` folder config.yml - this file is written to `integration_configs.{integration_name}` schema which is used in the integration.
- Silver and Gold Layer are tied to the same job separeted by tasks.
- Each alert (Bronze validation & price movement) has it's own job.

## Alerts
- There are two types of alerts, Bronze validation & price movement
- Bronze validation is set to fail if any anomalies come up in the Bronze Layer.
- Price movement thresholds are configured in `alerts/price_movement`.
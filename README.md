## Setup the workspace
1. Setup Databricks Workspace in Azure portal.
2. Connect with Github.
3. Create a cluster in Databricks Workspace Compute section.

## Architecture
Each integration operates through its dedicated job, responsible for ingesting data from the source into the Bronze Layer (`bronze_{source_system}` schema).

Bronze Layer:
- During this process, lakehouse_ fields (`lakehouse_pk` & `lakehouse_load_ts`) are added.

Silver Layer:
- The Silver Layer consolidates tables from each source under the `silver_stocks` schema. This phase involves cleaning, filtering, and applying minor aggregates to the data.

Gold Layer:
- In the Gold Layer, we conduct extensive aggregations and enrich the data further.

## Jobs
Job Management:
- Jobs are managed within Databricks Workflows, with each integration configured as its dedicated job.

Source Code:
- The source code for jobs is sourced from Github, defined by a Tag, created upon release (e.g., `v1.1.0`).

Integration Configuration:
- Integration configurations are located in the `project/{integration_name}/` folder's `config.yml`. This file is then written to the `integration_configs.{integration_name}` schema, which is used in the integration process.

Silver and Gold Layer:
- The Silver and Gold Layers are executed within the same job, separated by tasks.

Alert Mechanism:
- Each alert, whether for Bronze validation or price movement, operates within its dedicated job.

## Alerts
- There are two types of alerts, Bronze validation & price movement.
- Bronze validation is set to fail if any anomalies come up in the Bronze Layer.
- Price movement thresholds are configured in `alerts/price_movement`.
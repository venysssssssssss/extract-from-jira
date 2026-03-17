# Module Documentation

## extractor/config.py
Responsibility: load typed settings from environment and compute default extraction window ((D-1)-1 mês .. D-1).
Key classes/functions:
- `Settings`: central typed configuration with env aliases.
- `default_window()`: returns extraction window from `(D-1)-1 mês` up to `D-1`.

## extractor/domain.py
Responsibility: immutable domain contracts independent from frameworks.
Key classes:
- `BaseName`, `SourceMode`
- `ExtractionWindow`, `ExtractionRequest`
- `RecordEnvelope`, `BaseExecutionResult`, `ExtractionRunResult`

## extractor/exceptions.py
Responsibility: explicit error taxonomy for orchestration, API, fallback and validation paths.

## extractor/business_rules.py
Responsibility: business rules and filter metadata for each extraction base.
Key assets:
- `RULES`: configuration for `encerradas`, `analisadas`, `ingressadas`
- `REQUIRED_FIELD_NAMES`: required Jira custom fields to resolve before extraction

## extractor/interfaces.py
Responsibility: dependency inversion interfaces used by application service.
Contracts:
- `JiraGateway`, `FallbackGateway`, `StorageGateway`, `Auditor`, `Normalizer`

## extractor/utils.py
Responsibility: generic utility helpers.
- `canonicalize()`: accent-insensitive text normalization.

## extractor/logging_config.py
Responsibility: centralized logging setup and request context propagation.
Main behavior:
- Configures console + rotating file handlers.
- Supports plain text or JSON logs via env vars.
- Injects `request_id` into all records using context variables.

## extractor/jql_builder.py
Responsibility: deterministic JQL construction from business rules and extraction window.
- `build_jql()`

## extractor/jira_api_client.py
Responsibility: Jira REST implementation with retries and pagination.
Main behavior:
- Resolves custom field IDs with accent-insensitive matching.
- Executes paginated search requests.
- Raises typed exceptions for auth/transient/schema failures.

## extractor/normalizer.py
Responsibility: unify API and fallback records into a shared schema.
Main behavior:
- `normalize_api_issues()`: transforms raw Jira issue payload and expands custom fields per base.
- `normalize_fallback_csv()`: maps CSV export columns to the same schema and fills missing custom columns with `None`.
- `utc_now_iso()`: centralized timestamp formatting.

## extractor/validators.py
Responsibility: data quality validation.
Checks:
- Required schema columns per base (`11` core + custom fields)
- Nulls in critical columns
- `data_referencia` inside extraction window

## extractor/storage.py
Responsibility: file persistence for raw and processed layers.
Outputs:
- `raw/<base>/<from_date>__<to_date>.jsonl`
- `processed/<base>/<from_date>__<to_date>.csv`
- `processed/<base>/<from_date>__<to_date>.parquet`
- `processed/<base>/periodo_<from_date>__<to_date>.json`
Behavior:
- Deduplication by `issue_key + updated`.
- Reindexes outputs using dynamic schema for each base.

## extractor/sql_server_writer.py
Responsibility: write extracted records into SQL Server tables per base.
Main behavior:
- Creates `jira_<base>` table if it does not exist.
- Uses Portuguese column names in DB schema for core fields and canonicalized names for custom fields.
- Adds missing columns to existing tables via `ALTER TABLE ADD`.
- Performs full refresh per table on each run before inserting fresh rows.
- Verifies row count for data integrity after load.

## extractor/audit.py
Responsibility: append structured audit events in JSONL.
Output:
- `audit/extraction_audit_<date>.jsonl`

## extractor/playwright_fallback.py
Responsibility: browser automation fallback for Jira export when API fails.
Main behavior:
- Optional Atlassian login attempt.
- Export CSV from issue navigator.
- Saves file under `fallback/<base>/<date>/`.

## extractor/service.py
Responsibility: main orchestration use case (API-first, fallback on eligible API errors).
Main behavior:
- Clears `raw/processed/fallback` output folders for selected bases before each `api-first` execution (configurable).
- Resolves field IDs once per run.
- Extracts each base via API.
- Falls back to Playwright when API error is eligible.
- Validates, persists, and audits each base.

## extractor/bootstrap.py
Responsibility: composition root for wiring concrete dependencies.
- `build_service()`

## extractor/run.py
Responsibility: CLI entry point for manual/scheduled runs.

## api/schemas.py
Responsibility: FastAPI input/output schema definitions.

## api/main.py
Responsibility: HTTP service layer.
Main behavior:
- Adds request logging middleware with `X-Request-ID`.
- Uses centralized logging configuration.
Endpoints:
- `GET /healthz`
- `POST /v1/extractions/run`

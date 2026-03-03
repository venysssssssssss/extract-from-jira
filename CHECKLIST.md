# Delivery Checklist

- [x] Define project packaging and runtime dependencies (`pyproject.toml`).
- [x] Implement modular architecture with SOLID-oriented boundaries.
- [x] Implement Jira API gateway with retries, pagination, and field discovery.
- [x] Implement normalization, validation, storage, and structured audit modules.
- [x] Implement Playwright fallback module and fallback trigger handling.
- [x] Implement extraction orchestration service (`api-first`).
- [x] Implement CLI entry point (`python -m extractor.run`).
- [x] Implement FastAPI endpoints (`/healthz`, `/v1/extractions/run`).
- [x] Add automated tests for JQL, normalizer, service fallback, and API endpoint.
- [ ] Run local automated test suite and validate all checks passing. (Blocked: environment without internet access to install dependencies and without system `pytest`)
- [x] Add production containerization assets.
- [x] Update README with implementation and runtime instructions.

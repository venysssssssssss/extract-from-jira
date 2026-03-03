"""Jira REST gateway implementation with retries and pagination."""

from __future__ import annotations

import time
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from extractor.exceptions import (
    ApiAuthError,
    ApiSchemaError,
    ApiTransientError,
    ConfigurationError,
    ExtractionError,
)
from extractor.interfaces import JiraGateway
from extractor.utils import canonicalize


class JiraApiClient(JiraGateway):
    """Low-level Jira API client implementing field discovery and issue search."""

    def __init__(
        self,
        base_url: str,
        email: str,
        api_token: str,
        retry_attempts: int,
        retry_backoff_seconds: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._retry_attempts = retry_attempts
        self._retry_backoff = retry_backoff_seconds
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(email, api_token)
        self._session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        url = f"{self._base_url}{path}"
        last_error: Exception | None = None

        for attempt in range(1, self._retry_attempts + 1):
            try:
                response = self._session.request(
                    method, url, params=params, json=payload, timeout=60
                )
            except requests.RequestException as exc:
                last_error = ApiTransientError(f"Request to Jira failed: {exc}")
                if attempt == self._retry_attempts:
                    raise last_error from exc
                time.sleep(self._retry_backoff * attempt)
                continue

            if response.status_code in (401, 403):
                raise ApiAuthError(
                    f"Jira authentication/authorization failed ({response.status_code})"
                )
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                last_error = ApiTransientError(
                    f"Jira transient error {response.status_code}: {response.text[:300]}"
                )
                if attempt == self._retry_attempts:
                    raise last_error
                time.sleep(self._retry_backoff * attempt)
                continue
            if response.status_code >= 400:
                raise ExtractionError(
                    f"Jira API non-retriable error {response.status_code}: {response.text[:500]}"
                )

            try:
                return response.json()
            except ValueError as exc:
                raise ApiSchemaError("Jira API returned non-JSON payload") from exc

        if last_error is not None:
            raise last_error
        raise ApiTransientError("Jira API request failed without detailed error")

    def resolve_field_ids(self, field_names: tuple[str, ...]) -> dict[str, str]:
        """Resolve field IDs by display name using accent-insensitive matching."""

        payload = self._request("GET", "/rest/api/3/field")
        if not isinstance(payload, list):
            raise ApiSchemaError("Unexpected /field payload type")

        name_map: dict[str, str] = {}
        indexed: dict[str, str] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            field_id = str(item.get("id", "")).strip()
            if name and field_id:
                indexed[canonicalize(name)] = field_id

        missing: list[str] = []
        for name in field_names:
            key = canonicalize(name)
            if key not in indexed:
                missing.append(name)
                continue
            name_map[name] = indexed[key]

        if missing:
            raise ConfigurationError(
                f"Could not resolve Jira field ids for: {', '.join(missing)}"
            )
        return name_map

    def search_issues(
        self, jql: str, fields: tuple[str, ...], max_results: int
    ) -> list[dict[str, Any]]:
        """Fetch all Jira issues with paginated /search calls."""

        start_at = 0
        issues: list[dict[str, Any]] = []
        total: int | None = None

        while total is None or start_at < total:
            body = {
                "jql": jql,
                "fields": list(dict.fromkeys(fields)),
                "startAt": start_at,
                "maxResults": max_results,
            }
            payload = self._request("POST", "/rest/api/3/search", payload=body)
            if not isinstance(payload, dict):
                raise ApiSchemaError("Unexpected /search payload type")

            page_issues = payload.get("issues")
            total = payload.get("total")
            if not isinstance(page_issues, list) or not isinstance(total, int):
                raise ApiSchemaError("Jira /search payload missing issues or total")

            issues.extend(item for item in page_issues if isinstance(item, dict))
            start_at += max_results

            if not page_issues:
                break

        return issues

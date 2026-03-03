"""Jira REST gateway implementation with retries and pagination."""

from __future__ import annotations

import logging
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

LOGGER = logging.getLogger(__name__)


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
                LOGGER.warning(
                    "jira_request_network_error attempt=%s method=%s path=%s error=%s",
                    attempt,
                    method,
                    path,
                    exc,
                )
                if attempt == self._retry_attempts:
                    raise last_error from exc
                time.sleep(self._retry_backoff * attempt)
                continue

            if response.status_code in (401, 403):
                LOGGER.error(
                    "jira_request_auth_error method=%s path=%s status=%s",
                    method,
                    path,
                    response.status_code,
                )
                raise ApiAuthError(
                    f"Jira authentication/authorization failed ({response.status_code})"
                )
            if response.status_code == 429 or 500 <= response.status_code <= 599:
                last_error = ApiTransientError(
                    f"Jira transient error {response.status_code}: {response.text[:300]}"
                )
                LOGGER.warning(
                    "jira_request_transient_error attempt=%s method=%s path=%s status=%s",
                    attempt,
                    method,
                    path,
                    response.status_code,
                )
                if attempt == self._retry_attempts:
                    raise last_error
                time.sleep(self._retry_backoff * attempt)
                continue
            if response.status_code >= 400:
                LOGGER.error(
                    "jira_request_non_retriable_error method=%s path=%s status=%s",
                    method,
                    path,
                    response.status_code,
                )
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

        issues: list[dict[str, Any]] = []
        next_page_token: str | None = None

        while True:
            body = {
                "jql": jql,
                "fields": list(dict.fromkeys(fields)),
                "maxResults": max_results,
                "fieldsByKeys": False,
            }
            if next_page_token:
                body["nextPageToken"] = next_page_token

            payload = self._request("POST", "/rest/api/3/search/jql", payload=body)
            if not isinstance(payload, dict):
                raise ApiSchemaError("Unexpected /search/jql payload type")

            page_issues = payload.get("issues")
            if not isinstance(page_issues, list):
                raise ApiSchemaError("Jira /search/jql payload missing issues list")

            issues.extend(item for item in page_issues if isinstance(item, dict))
            next_token = payload.get("nextPageToken")
            next_page_token = (
                str(next_token) if isinstance(next_token, str) and next_token else None
            )

            if not page_issues or not next_page_token:
                break

        return issues

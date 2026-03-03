"""CLI entry point for on-demand extraction execution."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date

from dotenv import load_dotenv

from extractor.bootstrap import build_service
from extractor.config import Settings
from extractor.logging_config import configure_logging


LOGGER = logging.getLogger(__name__)


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def main() -> None:
    load_dotenv()
    settings = Settings()
    configure_logging(
        level=settings.log_level,
        json_format=settings.log_json,
        log_file=settings.log_file,
        max_bytes=settings.log_max_bytes,
        backup_count=settings.log_backup_count,
    )

    parser = argparse.ArgumentParser(description="Run Jira extraction pipeline")
    parser.add_argument(
        "--base",
        default="all",
        choices=("all", "encerradas", "analisadas", "ingressadas"),
    )
    parser.add_argument(
        "--from", dest="from_date", default=None, help="Start date YYYY-MM-DD"
    )
    parser.add_argument(
        "--to", dest="to_date", default=None, help="End date YYYY-MM-DD"
    )
    parser.add_argument("--mode", default="api-first", choices=("api-first",))
    parser.add_argument(
        "--format", dest="fmt", default="csv,parquet", help="Comma-separated formats"
    )

    args = parser.parse_args()
    formats = tuple(item.strip() for item in args.fmt.split(",") if item.strip())

    LOGGER.info(
        "cli_extraction_start base=%s mode=%s formats=%s from=%s to=%s",
        args.base,
        args.mode,
        formats,
        args.from_date,
        args.to_date,
    )
    service = build_service(settings)
    results = service.run(
        request_base=args.base,
        from_date=_parse_date(args.from_date),
        to_date=_parse_date(args.to_date),
        formats=formats,
        mode=args.mode,
    )
    LOGGER.info("cli_extraction_finished bases=%d", len(results))

    serializable = [
        {
            "base": item.base.value,
            "source_mode": item.source_mode.value,
            "total_records": item.total_records,
            "from_date": item.from_date.isoformat(),
            "to_date": item.to_date.isoformat(),
            "raw_path": item.raw_path,
            "csv_path": item.csv_path,
            "parquet_path": item.parquet_path,
            "started_at": item.started_at.isoformat(),
            "finished_at": item.finished_at.isoformat(),
        }
        for item in results
    ]
    print(json.dumps(serializable, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

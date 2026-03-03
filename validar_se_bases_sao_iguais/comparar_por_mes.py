"""Compare Jira reference XLSX files against extracted CSVs by month.

Usage:
    poetry run python validar_se_bases_sao_iguais/comparar_por_mes.py --run-date 2026-03-02
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import pandas as pd


XML_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}
CELL_REF_RE = re.compile(r"([A-Z]+)(\d+)")


@dataclass(frozen=True)
class BaseConfig:
    base: str
    xlsx_path: Path
    csv_dir: Path
    ref_date_column: str


BASE_CONFIGS: dict[str, BaseConfig] = {
    "encerradas": BaseConfig(
        base="encerradas",
        xlsx_path=Path("validar_se_bases_sao_iguais/encerradas.xlsx"),
        csv_dir=Path("output/processed/encerradas"),
        ref_date_column="DATA FECHOU SALESFORCE",
    ),
    "analisadas": BaseConfig(
        base="analisadas",
        xlsx_path=Path("validar_se_bases_sao_iguais/analisadas.xlsx"),
        csv_dir=Path("output/processed/analisadas"),
        ref_date_column="DATA ÚLTIMA ANÁLISE",
    ),
    "ingressadas": BaseConfig(
        base="ingressadas",
        xlsx_path=Path("validar_se_bases_sao_iguais/ingressadas.xlsx"),
        csv_dir=Path("output/processed/ingressadas"),
        ref_date_column="DATA DE ABERTURA",
    ),
}


def canonicalize(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(without_accents.lower().strip().split())


def excel_col_to_idx(col: str) -> int:
    result = 0
    for ch in col:
        result = result * 26 + (ord(ch) - 64)
    return result - 1


def _load_shared_strings(zipped: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zipped.namelist():
        return []
    root = ET.fromstring(zipped.read("xl/sharedStrings.xml"))
    return [
        "".join(t.text or "" for t in si.findall(".//main:t", XML_NS))
        for si in root.findall("main:si", XML_NS)
    ]


def _worksheet_path(zipped: ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(zipped.read("xl/workbook.xml"))
    rels = ET.fromstring(zipped.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels.findall("rel:Relationship", XML_NS)
    }

    rel_id = None
    for sheet in workbook.findall("main:sheets/main:sheet", XML_NS):
        if sheet.attrib.get("name") == sheet_name:
            rel_id = sheet.attrib.get(
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            )
            break
    if not rel_id:
        raise ValueError(f"Sheet '{sheet_name}' not found")

    target = rel_map[rel_id]
    return target if target.startswith("xl/") else f"xl/{target}"


def load_xlsx_sheet(path: Path, sheet_name: str) -> list[list[str]]:
    with ZipFile(path) as zipped:
        shared_strings = _load_shared_strings(zipped)
        worksheet_xml = ET.fromstring(zipped.read(_worksheet_path(zipped, sheet_name)))

    rows: list[list[str]] = []
    for row in worksheet_xml.findall("main:sheetData/main:row", XML_NS):
        values_by_index: dict[int, str] = {}
        for cell in row.findall("main:c", XML_NS):
            ref = cell.attrib.get("r", "A1")
            match = CELL_REF_RE.match(ref)
            if not match:
                continue
            col_idx = excel_col_to_idx(match.group(1))
            value = ""
            cell_type = cell.attrib.get("t")
            cell_v = cell.find("main:v", XML_NS)
            inline_str = cell.find("main:is", XML_NS)
            if cell_type == "s" and cell_v is not None and cell_v.text:
                shared_idx = int(cell_v.text)
                if 0 <= shared_idx < len(shared_strings):
                    value = shared_strings[shared_idx]
            elif cell_type == "inlineStr" and inline_str is not None:
                value = "".join(
                    node.text or "" for node in inline_str.findall(".//main:t", XML_NS)
                )
            elif cell_v is not None and cell_v.text is not None:
                value = cell_v.text
            values_by_index[col_idx] = value

        if not values_by_index:
            rows.append([])
            continue
        max_idx = max(values_by_index)
        rows.append([values_by_index.get(i, "") for i in range(max_idx + 1)])
    return rows


def _normalize_scalar(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _to_date(value: object) -> date | None:
    text = _normalize_scalar(value)
    if not text:
        return None

    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        number = float(text)
        if 30000 <= number <= 70000:
            return (datetime(1899, 12, 30) + timedelta(days=number)).date()

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _month_token(value: date | None) -> str | None:
    return value.strftime("%Y-%m") if value else None


def _select_csv_file(base_cfg: BaseConfig, run_date: str | None) -> Path:
    if run_date:
        selected = base_cfg.csv_dir / f"{run_date}.csv"
        if not selected.exists():
            raise FileNotFoundError(f"CSV not found for {base_cfg.base}: {selected}")
        return selected

    files = sorted(base_cfg.csv_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {base_cfg.csv_dir}")
    return files[-1]


def _load_reference_dataframe(base_cfg: BaseConfig) -> pd.DataFrame:
    rows = load_xlsx_sheet(base_cfg.xlsx_path, "Your Jira Issues")
    if not rows:
        return pd.DataFrame()

    header = rows[0]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        if not any(str(cell).strip() for cell in row):
            continue
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        records.append({header[idx]: row[idx] for idx in range(len(header))})
    return pd.DataFrame(records)


def _build_counter_from_local(df: pd.DataFrame) -> Counter[tuple[str, str, str]]:
    signatures: list[tuple[str, str, str]] = []
    for _, row in df.iterrows():
        ref_date = _to_date(row.get("data_referencia"))
        month = _month_token(ref_date)
        if not month:
            continue
        signatures.append(
            (
                canonicalize(_normalize_scalar(row.get("summary"))),
                canonicalize(_normalize_scalar(row.get("status"))),
                month,
            )
        )
    return Counter(signatures)


def _build_summary_counter_from_local(df: pd.DataFrame) -> Counter[tuple[str, str]]:
    signatures: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        ref_date = _to_date(row.get("data_referencia"))
        month = _month_token(ref_date)
        if not month:
            continue
        signatures.append((canonicalize(_normalize_scalar(row.get("summary"))), month))
    return Counter(signatures)


def _build_counter_from_reference(
    df: pd.DataFrame, ref_date_column: str
) -> Counter[tuple[str, str, str]]:
    signatures: list[tuple[str, str, str]] = []
    for _, row in df.iterrows():
        ref_date = _to_date(row.get(ref_date_column))
        month = _month_token(ref_date)
        if not month:
            continue
        signatures.append(
            (
                canonicalize(_normalize_scalar(row.get("Resumo"))),
                canonicalize(_normalize_scalar(row.get("Status"))),
                month,
            )
        )
    return Counter(signatures)


def _build_summary_counter_from_reference(
    df: pd.DataFrame, ref_date_column: str
) -> Counter[tuple[str, str]]:
    signatures: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        ref_date = _to_date(row.get(ref_date_column))
        month = _month_token(ref_date)
        if not month:
            continue
        signatures.append((canonicalize(_normalize_scalar(row.get("Resumo"))), month))
    return Counter(signatures)


def compare_base(
    base_cfg: BaseConfig,
    run_date: str | None,
    month: str | None,
    include_reference_only_months: bool,
) -> list[dict]:
    csv_path = _select_csv_file(base_cfg, run_date)
    local_df = pd.read_csv(csv_path)
    ref_df = _load_reference_dataframe(base_cfg)

    local_counter = _build_counter_from_local(local_df)
    ref_counter = _build_counter_from_reference(ref_df, base_cfg.ref_date_column)
    local_summary_counter = _build_summary_counter_from_local(local_df)
    ref_summary_counter = _build_summary_counter_from_reference(
        ref_df, base_cfg.ref_date_column
    )

    local_months = {sig[2] for sig in local_counter}
    ref_months = {sig[2] for sig in ref_counter}
    all_months = sorted(local_months | ref_months) if include_reference_only_months else sorted(local_months)
    if month:
        all_months = [m for m in all_months if m == month]

    rows: list[dict] = []
    for current_month in all_months:
        local_month = Counter(
            {k: v for k, v in local_counter.items() if k[2] == current_month}
        )
        ref_month = Counter(
            {k: v for k, v in ref_counter.items() if k[2] == current_month}
        )
        local_summary_month = Counter(
            {k: v for k, v in local_summary_counter.items() if k[1] == current_month}
        )
        ref_summary_month = Counter(
            {k: v for k, v in ref_summary_counter.items() if k[1] == current_month}
        )

        missing_in_local = ref_month - local_month
        extra_in_local = local_month - ref_month
        relaxed_missing = ref_summary_month - local_summary_month
        relaxed_extra = local_summary_month - ref_summary_month

        rows.append(
            {
                "base": base_cfg.base,
                "month": current_month,
                "csv_file": str(csv_path),
                "local_count": int(sum(local_month.values())),
                "reference_count": int(sum(ref_month.values())),
                "matched_count": int(sum((local_month & ref_month).values())),
                "missing_in_local_count": int(sum(missing_in_local.values())),
                "extra_in_local_count": int(sum(extra_in_local.values())),
                "relaxed_matched_count": int(
                    sum((local_summary_month & ref_summary_month).values())
                ),
                "relaxed_missing_count": int(sum(relaxed_missing.values())),
                "relaxed_extra_count": int(sum(relaxed_extra.values())),
                "missing_examples": [
                    {"summary": k[0], "status": k[1], "qty": v}
                    for k, v in missing_in_local.most_common(10)
                ],
                "extra_examples": [
                    {"summary": k[0], "status": k[1], "qty": v}
                    for k, v in extra_in_local.most_common(10)
                ],
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare extracted CSVs against reference XLSX files by month"
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="CSV token date in YYYY-MM-DD. If omitted, uses latest CSV per base.",
    )
    parser.add_argument(
        "--month",
        default=None,
        help="Month token YYYY-MM to restrict validation. If omitted, validates all months found.",
    )
    parser.add_argument(
        "--base",
        default="all",
        choices=("all", "encerradas", "analisadas", "ingressadas"),
        help="Base scope to validate.",
    )
    parser.add_argument(
        "--output-dir",
        default="validar_se_bases_sao_iguais/resultado",
        help="Directory where comparison reports will be written.",
    )
    parser.add_argument(
        "--include-reference-only-months",
        action="store_true",
        help="Also include months that exist only in the reference XLSX.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected_bases = (
        list(BASE_CONFIGS.keys()) if args.base == "all" else [args.base]
    )

    report_rows: list[dict] = []
    for base in selected_bases:
        report_rows.extend(
            compare_base(
                BASE_CONFIGS[base],
                run_date=args.run_date,
                month=args.month,
                include_reference_only_months=args.include_reference_only_months,
            )
        )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"comparacao_por_mes_{now_tag}.json"
    csv_path = output_dir / f"comparacao_por_mes_{now_tag}.csv"

    with json_path.open("w", encoding="utf-8") as handler:
        json.dump(report_rows, handler, ensure_ascii=False, indent=2)

    flat_fields = [
        "base",
        "month",
        "csv_file",
        "local_count",
        "reference_count",
        "matched_count",
        "missing_in_local_count",
        "extra_in_local_count",
        "relaxed_matched_count",
        "relaxed_missing_count",
        "relaxed_extra_count",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handler:
        writer = csv.DictWriter(handler, fieldnames=flat_fields)
        writer.writeheader()
        for row in report_rows:
            writer.writerow({field: row[field] for field in flat_fields})

    print(f"JSON report: {json_path}")
    print(f"CSV report : {csv_path}")
    print()
    for row in report_rows:
        print(
            f"[{row['base']} | {row['month']}] "
            f"local={row['local_count']} ref={row['reference_count']} "
            f"match={row['matched_count']} "
            f"missing={row['missing_in_local_count']} extra={row['extra_in_local_count']} | "
            f"relaxed_match={row['relaxed_matched_count']} "
            f"relaxed_missing={row['relaxed_missing_count']} "
            f"relaxed_extra={row['relaxed_extra_count']}"
        )


if __name__ == "__main__":
    main()

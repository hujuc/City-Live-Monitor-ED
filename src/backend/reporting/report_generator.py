"""Report generation helpers for assignment deliverables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from backend.storage.bronze import BronzeStorage
from backend.storage.gold import GoldStorage
from backend.storage.silver import SilverStorage
from config.settings import Config


def _safe_percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100.0, 2)


def _collect_parquet_profile(files: list[Path], sample_files: int, source_label: str) -> dict[str, Any]:
    selected = files[:sample_files]
    if not selected:
        return {
            "source": source_label,
            "files": 0,
            "sample_rows": 0,
            "columns": [],
            "null_rates": {},
            "dtypes": {},
        }

    frames = [pd.read_parquet(path) for path in selected]
    sample = pd.concat(frames, ignore_index=True)
    null_rates = {
        col: _safe_percent(int(sample[col].isna().sum()), len(sample)) for col in sample.columns
    }

    return {
        "source": source_label,
        "files": len(files),
        "sample_rows": len(sample),
        "columns": list(sample.columns),
        "null_rates": null_rates,
        "dtypes": {col: str(dtype) for col, dtype in sample.dtypes.items()},
    }


def _collect_ndjson_profile(files: list[Path], sample_files: int, source_label: str) -> dict[str, Any]:
    selected = files[:sample_files]
    rows: list[dict[str, Any]] = []
    for file_path in selected:
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))

    if not rows:
        return {
            "source": source_label,
            "files": 0,
            "sample_rows": 0,
            "columns": [],
            "null_rates": {},
            "dtypes": {},
        }

    sample = pd.json_normalize(rows)
    null_rates = {
        col: _safe_percent(int(sample[col].isna().sum()), len(sample)) for col in sample.columns
    }
    return {
        "source": source_label,
        "files": len(files),
        "sample_rows": len(sample),
        "columns": list(sample.columns),
        "null_rates": null_rates,
        "dtypes": {col: str(dtype) for col, dtype in sample.dtypes.items()},
    }


def _format_profile_section(profile: dict[str, Any]) -> list[str]:
    lines = [
        f"### {profile['source']}",
        "",
        f"- Ficheiros observados: {profile['files']}",
        f"- Linhas em amostra: {profile['sample_rows']}",
    ]

    if not profile["columns"]:
        lines.extend(["- Sem dados disponíveis para amostra.", ""])
        return lines

    lines.extend(["", "| Coluna | Tipo | Null % |", "|---|---|---:|"])
    for col in profile["columns"]:
        lines.append(
            f"| {col} | {profile['dtypes'].get(col, 'unknown')} | {profile['null_rates'].get(col, 0.0)} |"
        )
    lines.append("")
    return lines


def generate_data_profiling_report(output_path: Path | None = None, sample_files: int = 3) -> Path:
    """Generate a markdown profiling report for all ingested sources."""
    output = output_path or (Config.DOCS_DIR / "data-profiling-report.md")
    bronze = BronzeStorage()

    tlc_profile = _collect_parquet_profile(
        bronze.list_files(source="tlc", pattern="*.parquet"),
        sample_files=sample_files,
        source_label="TLC Batch (Bronze)",
    )
    events_profile = _collect_ndjson_profile(
        bronze.list_files(source="nyc_permitted_events", pattern="*.ndjson"),
        sample_files=sample_files,
        source_label="Permitted Events Stream (Bronze snapshots)",
    )
    noise_profile = _collect_ndjson_profile(
        bronze.list_files(source="nyc_311_noise", pattern="*.ndjson"),
        sample_files=sample_files,
        source_label="311 Noise Stream (Bronze snapshots)",
    )

    lines = [
        "# Data Profiling Report",
        "",
        "Gerado automaticamente a partir de amostras de dados Bronze.",
        "",
        "## Executive Summary",
        "",
        "- Cobertura: 1 fonte batch (TLC) + 2 fontes streaming (Events, 311 Noise)",
        "- Objetivo: identificar esquema, tipos e qualidade inicial dos dados",
        "",
        "## Source Profiles",
        "",
    ]

    for profile in [tlc_profile, events_profile, noise_profile]:
        lines.extend(_format_profile_section(profile))

    lines.extend(
        [
            "## Data Quality Issues (Initial)",
            "",
            "- Campos com null rate elevado devem ser priorizados para regras de limpeza no Silver.",
            "- Colunas de data/hora devem manter normalização para UTC de forma consistente.",
            "- Campos categóricos livres devem ter standardização (ex.: borough, complaint_type).",
            "",
            "## Cleaning Recommendations",
            "",
            "1. Aplicar validações de esquema e tipos no início da ingestão.",
            "2. Rejeitar para DLQ eventos sem timestamp ou event_id.",
            "3. Padronizar valores categóricos e manter mapeamento versionado.",
            "4. Reprocessar amostras periodicamente para detetar drift de schema.",
            "",
        ]
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


def generate_processing_report(output_path: Path | None = None) -> Path:
    """Generate a markdown processing report from current layer artifacts."""
    output = output_path or (Config.DOCS_DIR / "processing-report.md")

    bronze = BronzeStorage()
    silver = SilverStorage()
    gold = GoldStorage()

    bronze_tlc_files = bronze.list_files(source="tlc", pattern="*.parquet")
    bronze_events_files = bronze.list_files(source="nyc_permitted_events", pattern="*.ndjson")
    bronze_noise_files = bronze.list_files(source="nyc_311_noise", pattern="*.ndjson")

    silver_tlc_files = silver.list_files(source="tlc", pattern="*.parquet")
    silver_events_files = silver.list_files(source="events", pattern="*.parquet")
    silver_noise_files = silver.list_files(source="noise311", pattern="*.parquet")

    gold_tables = [
        "taxi_demand_by_zone",
        "event_schedule",
        "noise_by_borough_hour",
        Config.STREAM_AGGREGATOR_TABLE_NAME,
    ]

    lines = [
        "# Data Processing Report",
        "",
        "Gerado automaticamente com base nos artefactos atuais de Bronze/Silver/Gold.",
        "",
        "## Batch Processing Summary",
        "",
        f"- Bronze TLC files: {len(bronze_tlc_files)}",
        f"- Silver TLC files: {len(silver_tlc_files)}",
        f"- Bronze Events snapshots: {len(bronze_events_files)}",
        f"- Silver Events partitions: {len(silver_events_files)}",
        f"- Bronze Noise snapshots: {len(bronze_noise_files)}",
        f"- Silver Noise partitions: {len(silver_noise_files)}",
        "",
        "## Gold Tables",
        "",
        "| Table | Available | Rows |",
        "|---|---|---:|",
    ]

    for table in gold_tables:
        if gold.exists(table):
            rows = len(gold.read(table))
            lines.append(f"| {table} | yes | {rows} |")
        else:
            lines.append(f"| {table} | no | 0 |")

    lines.extend(
        [
            "",
            "## Stream Processing Summary",
            "",
            f"- Window size configured: {Config.STREAM_AGGREGATOR_WINDOW_MINUTES} minutes",
            f"- Dedup & out-of-order handled by EventProcessor: yes",
            f"- Aggregated metrics table: {Config.STREAM_AGGREGATOR_TABLE_NAME}",
            "",
            "## Notes",
            "",
            "- Para métricas de throughput/latência em runtime, capturar também logs operacionais do pipeline.",
            "- Este relatório é uma baseline automática para atualização antes da entrega final.",
            "",
        ]
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    return output

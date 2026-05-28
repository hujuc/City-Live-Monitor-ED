"""
CLI Entry Point

Command-line interface for the City Live Monitor platform.
"""

import time
from io import StringIO
from pathlib import Path

import click
import rich.box
import uvicorn
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from backend.ingestion.streaming import Noise311Poller, PermittedEventsPoller
from backend.processing.batch import (
    GoldEventScheduleProcessor,
    GoldNoiseAggregationProcessor,
    GoldStarSchemaProcessor,
    GoldTaxiDemandProcessor,
    Noise311SilverProcessor,
    PermittedEventsSilverProcessor,
    TLCBatchProcessor,
)
from backend.processing.streaming import KafkaAggregateRunner
from backend.reporting import generate_data_profiling_report, generate_processing_report
from backend.utils.bronze_validator import BronzeValidator
from backend.utils.logging import setup_logging
from config.settings import Config


def _render_rich_help(command: click.Command, ctx: click.Context, show_commands: bool) -> str:
    """Render Rich-formatted help output for a Click command or group."""
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=True)

    if command.help:
        console.print(Panel(command.help, title="Description", border_style="cyan"))

    usage_parts: list[str] = [ctx.command_path]
    if isinstance(command, click.MultiCommand):  # type: ignore[arg-type]
        usage_parts.append("[OPTIONS] COMMAND [ARGS]...")
    else:
        usage_parts.extend(command.collect_usage_pieces(ctx))
    usage = " ".join(str(part).strip() for part in usage_parts if str(part).strip())
    console.print(Panel(f"[bold]Usage:[/bold] {usage}", border_style="blue"))

    option_rows: list[tuple[str, str]] = []
    arg_rows: list[tuple[str, str]] = []

    for param in command.get_params(ctx):
        if isinstance(param, click.Option):
            names = ", ".join([*param.opts, *param.secondary_opts])
            default = param.get_default(ctx)
            default_str = ""
            if param.show_default and default not in (None, (), []):
                default_str = f" [default: {default}]"
            detail = (param.help or "").strip()
            detail = f"{detail}{default_str}".strip()
            option_rows.append((names, detail or "-"))
        elif isinstance(param, click.Argument):
            required = "required" if param.required else "optional"
            arg_rows.append(((param.name or "").upper(), required))

    def _make_table(col1: str, col2: str) -> Table:
        t = Table(box=rich.box.SIMPLE_HEAD, show_header=True, show_edge=False, pad_edge=False)
        t.add_column(col1, style="cyan bold", no_wrap=True)
        t.add_column(col2, style="white")
        return t

    if arg_rows:
        arg_table = _make_table("Argument", "Required?")
        for name, details in arg_rows:
            arg_table.add_row(name, details)
        console.print()
        console.print(
            Panel(arg_table, title="[bold green]Arguments[/bold green]", border_style="green")
        )

    if option_rows:
        opt_table = _make_table("Option", "Description")
        for names, details in option_rows:
            opt_table.add_row(names, details)
        console.print()
        console.print(
            Panel(opt_table, title="[bold green]Options[/bold green]", border_style="green")
        )

    if show_commands and isinstance(command, click.MultiCommand):  # type: ignore[arg-type]
        command_rows: list[tuple[str, str]] = []
        for cmd_name in command.list_commands(ctx):  # type: ignore[arg-type]
            sub = command.get_command(ctx, cmd_name)  # type: ignore[arg-type]
            if not sub:
                continue
            short_help = sub.get_short_help_str()
            command_rows.append((cmd_name, short_help or "-"))

        if command_rows:
            cmd_table = _make_table("Command", "Description")
            for name, desc in command_rows:
                cmd_table.add_row(name, desc)
            console.print()
            console.print(
                Panel(cmd_table, title="[bold green]Commands[/bold green]", border_style="green")
            )

    return buffer.getvalue()


class RichCommand(click.Command):
    """Click command with Rich-formatted help output."""

    def get_help(self, ctx: click.Context) -> str:
        return _render_rich_help(self, ctx, show_commands=False)


class RichGroup(click.Group):
    """Click group with Rich-formatted help output."""

    command_class = RichCommand

    def get_help(self, ctx: click.Context) -> str:
        return _render_rich_help(self, ctx, show_commands=True)


@click.group(cls=RichGroup)
@click.option(
    "--log-level",
    default=Config.LOG_LEVEL,
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.version_option(version="0.1.0")
def cli(log_level: str) -> None:
    """City Live Monitor — data pipeline CLI."""
    setup_logging(log_level=log_level, log_file=Config.LOGS_DIR / "pipeline.log")


@cli.command("api")
@click.option("--host", default=Config.API_HOST, show_default=True)
@click.option("--port", default=Config.API_PORT, show_default=True)
def api(host: str, port: int) -> None:
    """Start the FastAPI REST server."""
    uvicorn.run("backend.api.main:app", host=host, port=port, reload=True)


@cli.command("stream-events")
@click.option("--once/--daemon", default=True, show_default=True)
@click.option("--max-pages", type=int, default=None, help="Cap pagination for testing/demos")
def stream_events(once: bool, max_pages: int | None) -> None:
    """Run Permitted Events poller one time or in continuous mode."""
    try:
        poller = PermittedEventsPoller(max_pages=max_pages)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        if once:
            poller.ingest()
            return

        logger.info("[cli] Starting events poller in daemon mode")
        while True:
            poller.ingest()
            time.sleep(Config.EVENTS_POLL_SECONDS)
    finally:
        poller.close()


@cli.command("stream-noise311")
@click.option("--once/--daemon", default=True, show_default=True)
@click.option("--max-pages", type=int, default=None, help="Cap pagination for testing/demos")
def stream_noise311(once: bool, max_pages: int | None) -> None:
    """Run 311 Noise poller one time or in continuous mode."""
    try:
        poller = Noise311Poller(max_pages=max_pages)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        if once:
            poller.ingest()
            return

        logger.info("[cli] Starting 311 noise poller in daemon mode")
        while True:
            poller.ingest()
            time.sleep(Config.NOISE311_POLL_SECONDS)
    finally:
        poller.close()


@cli.command("stream-aggregates")
@click.option("--once/--daemon", default=True, show_default=True)
@click.option("--max-messages", default=500, show_default=True, type=int)
def stream_aggregates(once: bool, max_messages: int) -> None:
    """Consume Kafka events and persist 5-minute streaming aggregate metrics to Gold."""
    runner = KafkaAggregateRunner()

    if once:
        stats = runner.run_once(max_messages=max_messages)
        logger.info(
            "[cli] stream-aggregates once: seen={} rows_written={}",
            stats["seen"],
            stats["rows_written"],
        )
        return

    logger.info("[cli] Starting stream aggregator in daemon mode")
    while True:
        stats = runner.run_once(max_messages=max_messages)
        logger.info(
            "[cli] stream-aggregates cycle: seen={} rows_written={}",
            stats["seen"],
            stats["rows_written"],
        )
        time.sleep(Config.STREAM_AGGREGATOR_DAEMON_SLEEP_SECONDS)


@cli.command("silver-process-tlc")
@click.option("--taxi-type", default="yellow", show_default=True)
@click.option("--max-files", type=int, default=None)
def silver_process_tlc(taxi_type: str, max_files: int | None) -> None:
    """Process TLC Bronze parquet files into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "tlc" / taxi_type
    destination = Path(f"tlc/{taxi_type}")
    processor = TLCBatchProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] silver-process-tlc finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("silver-process-events")
@click.option("--max-files", type=int, default=None)
def silver_process_events(max_files: int | None) -> None:
    """Process Permitted Events Bronze NDJSON snapshots into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "nyc_permitted_events"
    destination = Path("events")
    processor = PermittedEventsSilverProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] silver-process-events finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("silver-process-noise311")
@click.option("--max-files", type=int, default=None)
def silver_process_noise311(max_files: int | None) -> None:
    """Process 311 Noise Bronze NDJSON snapshots into Silver partitions."""
    bronze_root = Config.BRONZE_DIR / "nyc_311_noise"
    destination = Path("noise311")
    processor = Noise311SilverProcessor()

    stats = processor.process_bronze_tree(
        bronze_root=bronze_root,
        destination=destination,
        max_files=max_files,
    )
    logger.info(
        "[cli] silver-process-noise311 finished: files={} rows_in={} rows_out={} dropped={}",
        stats.files_processed,
        stats.rows_read,
        stats.rows_written,
        stats.rows_dropped,
    )


@cli.command("gold-build-taxi-demand")
def gold_build_taxi_demand() -> None:
    """Aggregate Silver TLC trips into Gold taxi demand table."""
    rows = GoldTaxiDemandProcessor().build()
    logger.info("[cli] gold-build-taxi-demand finished: {} rows written", rows)


@cli.command("gold-build-event-schedule")
def gold_build_event_schedule() -> None:
    """Build Gold event schedule table from Silver events."""
    rows = GoldEventScheduleProcessor().build()
    logger.info("[cli] gold-build-event-schedule finished: {} rows written", rows)


@cli.command("gold-build-noise")
def gold_build_noise() -> None:
    """Aggregate Silver noise311 complaints into Gold noise table."""
    rows = GoldNoiseAggregationProcessor().build()
    logger.info("[cli] gold-build-noise finished: {} rows written", rows)


@cli.command("gold-build-star-schema")
def gold_build_star_schema() -> None:
    """Build Gold star schema tables: dim_zone, dim_time, fact_taxi_demand."""
    counts = GoldStarSchemaProcessor().build()
    logger.info(
        "[cli] gold-build-star-schema finished: dim_zone={} dim_time={} fact_taxi_demand={}",
        counts.get("dim_zone", 0),
        counts.get("dim_time", 0),
        counts.get("fact_taxi_demand", 0),
    )


@cli.command("gold-build-all")
def gold_build_all() -> None:
    """Build all Gold tables (flat aggregates + star schema) in sequence."""
    taxi_rows = GoldTaxiDemandProcessor().build()
    event_rows = GoldEventScheduleProcessor().build()
    noise_rows = GoldNoiseAggregationProcessor().build()
    star_counts = GoldStarSchemaProcessor().build()
    logger.info(
        "[cli] gold-build-all finished: taxi={} events={} noise={} dim_zone={} dim_time={} fact={}",
        taxi_rows,
        event_rows,
        noise_rows,
        star_counts.get("dim_zone", 0),
        star_counts.get("dim_time", 0),
        star_counts.get("fact_taxi_demand", 0),
    )


@cli.command("report-data-profiling")
@click.option("--sample-files", default=3, show_default=True, type=int)
def report_data_profiling(sample_files: int) -> None:
    """Generate docs/data-profiling-report.md from Bronze samples."""
    output = generate_data_profiling_report(sample_files=sample_files)
    logger.info("[cli] report-data-profiling finished: {}", output)


@cli.command("report-processing")
def report_processing() -> None:
    """Generate docs/processing-report.md from current pipeline artifacts."""
    output = generate_processing_report()
    logger.info("[cli] report-processing finished: {}", output)


@cli.command("backfill-bronze-meta")
@click.option("--dry-run", is_flag=True, default=False, help="List files without writing")
def backfill_bronze_meta(dry_run: bool) -> None:
    """Create .meta.json sidecars for TLC Parquet files that don't have one yet."""
    import json
    from datetime import UTC, datetime

    bronze_tlc = Config.BRONZE_DIR / "tlc"
    files = sorted(bronze_tlc.rglob("*.parquet"))
    created = 0
    skipped = 0

    for parquet_path in files:
        meta_path = parquet_path.with_suffix(".meta.json")
        if meta_path.exists():
            skipped += 1
            continue
        # Use file mtime as proxy ingestion timestamp for pre-existing files
        mtime = parquet_path.stat().st_mtime
        ingestion_ts = datetime.fromtimestamp(mtime, tz=UTC).isoformat()
        meta = {
            "_ingestion_timestamp": ingestion_ts,
            "source_file": parquet_path.name,
            "size_bytes": parquet_path.stat().st_size,
            "_backfilled": True,
        }
        if dry_run:
            logger.info("[backfill] would create {} (ts={})", meta_path.name, ingestion_ts)
        else:
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            logger.info("[backfill] created {}", meta_path.name)
        created += 1

    logger.info(
        "[cli] backfill-bronze-meta: created={} skipped={} dry_run={}",
        created,
        skipped,
        dry_run,
    )


@cli.command("backfill-ndjson-meta")
@click.option("--dry-run", is_flag=True, default=False, help="List files without modifying")
def backfill_ndjson_meta(dry_run: bool) -> None:
    """Inject _ingestion_timestamp into NDJSON rows that are missing it (uses file mtime)."""
    import json
    from datetime import UTC, datetime

    sources = [
        Config.BRONZE_DIR / "nyc_permitted_events",
        Config.BRONZE_DIR / "nyc_311_noise",
    ]
    patched = 0
    skipped = 0

    for source_dir in sources:
        for ndjson_path in sorted(source_dir.rglob("*.ndjson")):
            # Check first row only to decide whether to process
            with ndjson_path.open(encoding="utf-8") as fh:
                first = next((json.loads(line) for line in fh if line.strip()), None)
            if first is None or "_ingestion_timestamp" in first:
                skipped += 1
                continue

            mtime = ndjson_path.stat().st_mtime
            ingestion_ts = datetime.fromtimestamp(mtime, tz=UTC).isoformat()

            if dry_run:
                logger.info(
                    "[backfill-ndjson] would patch {} (ts={})", ndjson_path.name, ingestion_ts
                )
                patched += 1
                continue

            lines = ndjson_path.read_text(encoding="utf-8").splitlines()
            patched_lines = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                row.setdefault("_ingestion_timestamp", ingestion_ts)
                row["_backfilled"] = True
                patched_lines.append(json.dumps(row, ensure_ascii=False))
            ndjson_path.write_text("\n".join(patched_lines) + "\n", encoding="utf-8")
            logger.info("[backfill-ndjson] patched {}", ndjson_path.name)
            patched += 1

    logger.info("[cli] c: patched={} skipped={} dry_run={}", patched, skipped, dry_run)


@cli.command("validate-bronze")
def validate_bronze() -> None:
    """Validate Bronze layer: _ingestion_timestamp presence and schema consistency."""
    validator = BronzeValidator()

    results = [
        validator.validate_parquet_source(Config.BRONZE_DIR / "tlc", "tlc"),
        validator.validate_ndjson_source(Config.BRONZE_DIR / "nyc_permitted_events", "events"),
        validator.validate_ndjson_source(Config.BRONZE_DIR / "nyc_311_noise", "noise311"),
    ]

    all_valid = all(r.is_valid for r in results)
    total_issues = sum(len(r.missing_timestamp_files) + len(r.schema_drift_files) for r in results)
    logger.info(
        "[cli] validate-bronze finished: sources={} valid={} issues={}",
        len(results),
        all_valid,
        total_issues,
    )
    if not all_valid:
        raise SystemExit(1)


_TITLE_STYLE = "bold cyan"


def _human_size(num_bytes: int) -> str:
    """Format a byte count as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024  # type: ignore[assignment]
    return f"{num_bytes:.1f} PB"


def _dir_size_and_count(path: Path, pattern: str = "**/*") -> tuple[int, int]:
    """Return (total_bytes, file_count) for a directory tree matching pattern."""
    total = 0
    count = 0
    if not path.exists():
        return 0, 0
    for p in path.rglob(pattern):
        if p.is_file():
            total += p.stat().st_size
            count += 1
    return total, count


@cli.command("summary")
def summary() -> None:
    """Show a snapshot of Bronze/Silver/Gold state — counts, rows, disk usage."""
    import pandas as pd
    from rich.console import Console

    console = Console()

    bronze_sources = {
        "tlc": "TLC Yellow Taxi",
        "nyc_permitted_events": "NYC Permitted Events",
        "nyc_311_noise": "311 Noise Complaints",
    }
    bronze_table = Table(
        title="Bronze Layer (raw)",
        box=rich.box.SIMPLE_HEAD,
        title_style=_TITLE_STYLE,
    )
    bronze_table.add_column("Source", style="cyan")
    bronze_table.add_column("Files", justify="right")
    bronze_table.add_column("Size", justify="right")
    for key, label in bronze_sources.items():
        size, files = _dir_size_and_count(Config.BRONZE_DIR / key)
        bronze_table.add_row(label, f"{files:,}", _human_size(size) if files else "—")
    console.print(bronze_table)

    silver_table = Table(
        title="Silver Layer (cleaned)",
        box=rich.box.SIMPLE_HEAD,
        title_style=_TITLE_STYLE,
    )
    silver_table.add_column("Source", style="cyan")
    silver_table.add_column("Partitions", justify="right")
    silver_table.add_column("Size", justify="right")
    for key, label in {"tlc": "TLC", "events": "Events", "noise311": "311 Noise"}.items():
        size, files = _dir_size_and_count(Config.SILVER_DIR / key, "*.parquet")
        silver_table.add_row(label, f"{files:,}", _human_size(size) if files else "—")
    console.print(silver_table)

    gold_table = Table(
        title="Gold Layer (analytics-ready)",
        box=rich.box.SIMPLE_HEAD,
        title_style=_TITLE_STYLE,
    )
    gold_table.add_column("Table", style="cyan")
    gold_table.add_column("Rows", justify="right")
    gold_table.add_column("Columns", justify="right")
    gold_table.add_column("Size", justify="right")
    gold_files = sorted(Config.GOLD_DIR.rglob("*.parquet")) if Config.GOLD_DIR.exists() else []
    if not gold_files:
        gold_table.add_row("—", "—", "—", "—")
    for f in gold_files:
        try:
            df = pd.read_parquet(f)
            gold_table.add_row(
                f.parent.name, f"{len(df):,}", str(len(df.columns)), _human_size(f.stat().st_size)
            )
        except Exception as exc:
            gold_table.add_row(f.parent.name, "error", "-", str(exc)[:30])
    console.print(gold_table)

    # Checkpoints — shows streaming pipeline state
    checkpoints = (
        list(Config.CHECKPOINTS_DIR.glob("*.txt")) if Config.CHECKPOINTS_DIR.exists() else []
    )
    if checkpoints:
        cp_table = Table(
            title="Streaming Checkpoints",
            box=rich.box.SIMPLE_HEAD,
            title_style=_TITLE_STYLE,
        )
        cp_table.add_column("File", style="cyan")
        cp_table.add_column("Cursor")
        for cp in checkpoints:
            cp_table.add_row(cp.name, cp.read_text().strip())
        console.print(cp_table)


@cli.command("backfill-events-daily-counts")
@click.option(
    "--start-date",
    default="2024-06-01",
    show_default=True,
    help="Inclusive start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    default="2026-05-27",
    show_default=True,
    help="Inclusive end date (YYYY-MM-DD)",
)
def backfill_events_daily_counts(start_date: str, end_date: str) -> None:
    """Fetch per-day event counts from bkfu-528j via Socrata aggregation and save to Gold.

    Uses server-side grouping so only ~730 rows are transferred instead of
    millions of individual permit records. Output is saved to
    data/gold/ml_training/events_historical_counts.parquet and is picked up
    automatically by the train-model command.
    """
    import pandas as pd
    import requests

    url = Config.EVENTS_HISTORICAL_API_URL
    headers = {"X-App-Token": Config.SOCRATA_APP_TOKEN} if Config.SOCRATA_APP_TOKEN else {}

    # The dataset has 32M rows — a single GROUP BY times out; query month-by-month instead.
    # Results are checkpointed so a restart skips already-fetched months.
    out = Config.GOLD_DIR / "ml_training" / "events_historical_counts.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    existing: set[str] = set()
    if out.exists():
        prev = pd.read_parquet(out)
        existing = set(prev["date"].dt.strftime("%Y-%m"))

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    months = pd.period_range(start=start_ts, end=end_ts, freq="M")

    def _save(rows: list[dict]) -> None:
        """Merge new rows into the parquet and write atomically."""
        new_df = pd.DataFrame(rows)
        new_df["date"] = pd.to_datetime(new_df["event_date"], utc=True)
        new_df["events_count"] = (
            pd.to_numeric(new_df["events_count"], errors="coerce").fillna(0).astype(int)
        )
        parts = [new_df[["date", "events_count"]]]
        if out.exists():
            parts.insert(0, pd.read_parquet(out))
        merged = (
            pd.concat(parts, ignore_index=True)
            .groupby("date")["events_count"]
            .sum()
            .reset_index()
            .sort_values("date")
            .reset_index(drop=True)
        )
        merged.to_parquet(out, index=False)

    fetched_any = False
    for period in months:
        period_key = str(period)
        if period_key in existing:
            logger.info("[backfill-events-counts] Skipping {} (already fetched)", period_key)
            continue

        mo_start = period.start_time.strftime("%Y-%m-%dT00:00:00")
        mo_end = period.end_time.strftime("%Y-%m-%dT23:59:59")
        params = {
            "$select": "date_trunc_ymd(start_date_time) AS event_date, count(*) AS events_count",
            "$where": f"start_date_time >= '{mo_start}' AND start_date_time <= '{mo_end}'",
            "$group": "event_date",
            "$order": "event_date ASC",
            "$limit": "100",
        }
        for attempt in range(1, 4):
            try:
                logger.info(
                    "[backfill-events-counts] Fetching {} (attempt {})", period_key, attempt
                )
                resp = requests.get(url, params=params, headers=headers, timeout=240)
                resp.raise_for_status()
                month_rows = resp.json()
                if month_rows:
                    _save(month_rows)
                    existing.add(period_key)
                    fetched_any = True
                break
            except requests.exceptions.Timeout:
                if attempt == 3:
                    logger.error(
                        "[backfill-events-counts] {} timed out after 3 attempts", period_key
                    )
                    raise
                logger.warning("[backfill-events-counts] Timeout on {}; retrying…", period_key)

    if not fetched_any and not out.exists():
        click.echo("No data returned — check date range or network.")
        return

    df = pd.read_parquet(out)
    click.echo(f"Saved {len(df)} daily rows to {out}")
    if not df.empty:
        click.echo(f"Date range: {df['date'].min().date()} -> {df['date'].max().date()}")
        click.echo(
            f"events_count range: {df['events_count'].min()} - {df['events_count'].max()}"
            f"  (median {int(df['events_count'].median())})"
        )


@cli.command("train-model")
@click.option("--start-date", default="2024-06-01", show_default=True, help="Training window start")
@click.option("--end-date", default="2026-05-27", show_default=True, help="Training window end")
def train_model(start_date: str, end_date: str) -> None:
    """Build ML training dataset from Silver and train an activity classifier."""
    from backend.ml.classifier import train_classifier
    from backend.processing.batch.gold.ml_dataset_builder import build_ml_dataset

    dataset_path = Config.GOLD_DIR / "ml_training" / "ml_dataset.parquet"
    model_path = Config.GOLD_DIR / "ml_training" / "activity_classifier.pkl"

    events_counts_path = Config.GOLD_DIR / "ml_training" / "events_historical_counts.parquet"
    build_ml_dataset(Config.SILVER_DIR, dataset_path, start_date, end_date, events_counts_path)
    results = train_classifier(dataset_path, model_path)

    click.echo(f"\nTrain size: {results['train_size']}  Test size: {results['test_size']}")
    click.echo(f"Threshold : {results['threshold']:.0f} complaints/day (training median)")
    click.echo("\nRandom Forest (test set):")
    click.echo(f"  Accuracy : {results['accuracy']:.2%}")
    click.echo(f"  Precision: {results['precision']:.3f}")
    click.echo(f"  Recall   : {results['recall']:.3f}")
    click.echo(f"  F1       : {results['f1']:.3f}")
    click.echo(f"  ROC-AUC  : {results['roc_auc']:.3f}")
    click.echo(
        f"\nCV ROC-AUC (TimeSeriesSplit n=5): {results['cv_mean']:.3f} ± {results['cv_std']:.3f}"
    )
    click.echo(f"  Folds: {[f'{s:.3f}' for s in results['cv_scores']]}")

    click.echo("\nBaselines:")
    for name, b in results["baselines"].items():
        auc = f"  ROC-AUC={b['roc_auc']:.3f}" if "roc_auc" in b else ""
        click.echo(f"  {name:22s}  acc={b['accuracy']:.2%}  f1={b['f1']:.3f}{auc}")

    click.echo("\nFeature importances:")
    for feat, imp in sorted(results["importances"].items(), key=lambda x: -x[1]):
        bar = "█" * int(imp * 40)
        click.echo(f"  {feat:30s} {imp:.4f}  {bar}")


if __name__ == "__main__":
    cli()

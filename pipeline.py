"""DataSift — Intelligent Data Enrichment Pipeline — main orchestrator."""

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

from stages.clean import clean
from stages.enrich import enrich
from stages.export import export
from stages.ingest import ingest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def load_config(config_path: str | Path) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_pipeline(config_path: str, input_override: str | None = None, dry_run: bool = False) -> None:
    log.info("=" * 60)
    log.info("DataSift Pipeline Starting")
    log.info("=" * 60)

    config = load_config(config_path)
    pipeline_cfg = config.get("pipeline", {})
    cleaning_cfg = config.get("cleaning", {})
    enrichment_cfg = config.get("enrichment", {})

    pipeline_name = pipeline_cfg.get("name", "DataSift Pipeline")
    input_path = input_override or pipeline_cfg.get("input_path")
    output_dir = pipeline_cfg.get("output_dir", "output")

    if not input_path:
        log.error("No input_path specified in config or --input argument")
        sys.exit(1)

    log.info(f"Pipeline: {pipeline_name}")
    log.info(f"Input:    {input_path}")
    log.info(f"Output:   {output_dir}")
    log.info(f"Dry run:  {dry_run}")
    log.info("-" * 60)

    # Stage 1: Ingest
    log.info("[Stage 1/4] Ingest")
    df = ingest(input_path)

    # Stage 2: Clean
    log.info("[Stage 2/4] Clean")
    df, cleaning_report = clean(df, cleaning_cfg)

    if dry_run:
        log.info("DRY RUN — skipping enrichment and export (no API calls)")
        log.info(f"Would enrich column: '{enrichment_cfg.get('input_column')}'")
        log.info(f"Would add columns: {list(enrichment_cfg.get('output_columns', {}).keys())}")
        log.info("Validation complete.")
        return

    if not os.getenv("OPENAI_API_KEY"):
        log.error("OPENAI_API_KEY not set. Set it in .env or environment.")
        sys.exit(1)

    # Stage 3: Enrich
    log.info("[Stage 3/4] Enrich")
    df, enrichment_report = enrich(df, enrichment_cfg)

    # Stage 4: Export
    log.info("[Stage 4/4] Export")
    outputs = export(
        df=df,
        output_dir=output_dir,
        pipeline_name=pipeline_name,
        enrichment_report=enrichment_report,
        cleaning_report=cleaning_report,
        enrichment_config=enrichment_cfg,
    )

    log.info("=" * 60)
    log.info("Pipeline Complete")
    log.info(f"  CSV:    {outputs['csv']}")
    log.info(f"  DB:     {outputs['db']}")
    log.info(f"  Report: {outputs['report']}")
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="DataSift — Intelligent Data Enrichment Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --config sample_data/pipeline_config.yaml
  python pipeline.py --config sample_data/pipeline_config.yaml --dry-run
  python pipeline.py --config sample_data/pipeline_config.yaml --input my_data.csv
""",
    )
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config file")
    parser.add_argument("--input", help="Override input file path from config")
    parser.add_argument("--dry-run", action="store_true", help="Validate only — no API calls")
    args = parser.parse_args()

    run_pipeline(
        config_path=args.config,
        input_override=args.input,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

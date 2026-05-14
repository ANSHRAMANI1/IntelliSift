"""Stage 1 — Ingest: load a CSV or JSON file into a Pandas DataFrame."""

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def ingest(input_path: str | Path) -> pd.DataFrame:
    """Load a CSV or JSON file and return a DataFrame."""
    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str)
    elif suffix in (".json", ".jsonl"):
        df = pd.read_json(path, dtype=str, lines=(suffix == ".jsonl"))
    else:
        raise ValueError(f"Unsupported file type: {suffix}. Expected .csv or .json")

    log.info(f"Ingested {len(df):,} rows × {len(df.columns)} columns from {path.name}")
    log.info(f"Columns: {list(df.columns)}")

    return df

"""Stage 2 — Clean: validate and clean the ingested DataFrame."""

import logging
from dataclasses import dataclass, field

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class CleaningReport:
    original_rows: int = 0
    rows_after_dedup: int = 0
    rows_after_required: int = 0
    missing_values_filled: int = 0
    text_columns_normalized: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


def clean(df: pd.DataFrame, cleaning_config: dict) -> tuple[pd.DataFrame, CleaningReport]:
    """
    Apply cleaning steps defined in the pipeline config.
    Returns the cleaned DataFrame and a CleaningReport.
    """
    report = CleaningReport(original_rows=len(df))
    df = df.copy()

    # Deduplicate
    if cleaning_config.get("drop_duplicates", True):
        before = len(df)
        df = df.drop_duplicates()
        removed = before - len(df)
        if removed:
            log.info(f"  Removed {removed} duplicate rows")
            report.issues.append(f"Removed {removed} duplicate rows")
    report.rows_after_dedup = len(df)

    # Drop rows missing required columns
    required = cleaning_config.get("required_columns", [])
    if required:
        before = len(df)
        df = df.dropna(subset=required)
        removed = before - len(df)
        if removed:
            log.info(f"  Dropped {removed} rows missing required columns {required}")
            report.issues.append(f"Dropped {removed} rows with missing required values")
    report.rows_after_required = len(df)

    # Normalize text columns
    text_columns = cleaning_config.get("text_columns", [])
    for col in text_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )
            report.text_columns_normalized.append(col)
            log.info(f"  Normalized text column: {col}")

    # Fill remaining nulls with empty string for text cols, 0 for numeric
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            if df[col].dtype == object:
                df[col] = df[col].fillna("")
            else:
                df[col] = df[col].fillna(0)
            report.missing_values_filled += null_count

    if report.missing_values_filled:
        log.info(f"  Filled {report.missing_values_filled} missing values")

    log.info(f"Cleaning complete: {report.original_rows} → {len(df)} rows")
    return df, report

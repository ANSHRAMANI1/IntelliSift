"""Stage 3 — Enrich: use GPT-4o to classify and tag each row in batches."""

import json
import logging
import os
import time
from dataclasses import dataclass, field

import pandas as pd
from openai import OpenAI
from tqdm import tqdm

log = logging.getLogger(__name__)


@dataclass
class EnrichmentReport:
    total_rows: int = 0
    enriched_rows: int = 0
    failed_rows: int = 0
    columns_added: list[str] = field(default_factory=list)


def _build_system_prompt(output_columns: dict) -> str:
    """Build a GPT-4o system prompt from the output_columns config."""
    col_descriptions = []
    for col_name, spec in output_columns.items():
        desc = spec.get("description", col_name)
        values = spec.get("values")
        col_type = spec.get("type", "classification")

        if values:
            col_descriptions.append(f'- "{col_name}": {desc}. Must be one of: {values}')
        else:
            col_descriptions.append(f'- "{col_name}": {desc}. Return a short text string.')

    fields_text = "\n".join(col_descriptions)

    return f"""You are a data enrichment assistant. For each text input provided, return a JSON object with these fields:

{fields_text}

Rules:
- Return ONLY a JSON array — one object per input text, in the same order
- For classification fields, return exactly one of the allowed values
- For text fields, be concise (max 20 words)
- Never skip an item — if unsure, make your best guess
"""


def _call_gpt_batch(
    client: OpenAI,
    texts: list[str],
    system_prompt: str,
    model: str,
    max_retries: int,
) -> list[dict]:
    """Send a batch of texts to GPT-4o and return parsed JSON results."""
    numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(texts))
    user_content = f"Texts to enrich:\n\n{numbered}"

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content
            parsed = json.loads(raw)

            # GPT may return {"results": [...]} or just [...]
            if isinstance(parsed, dict):
                for key in ("results", "items", "data", "enrichments"):
                    if key in parsed and isinstance(parsed[key], list):
                        parsed = parsed[key]
                        break

            if isinstance(parsed, list):
                return parsed

            # Fallback: return empty dicts
            return [{} for _ in texts]

        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"  Batch attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    log.error(f"  Batch failed after {max_retries} attempts")
    return [{} for _ in texts]


def enrich(df: pd.DataFrame, enrichment_config: dict) -> tuple[pd.DataFrame, EnrichmentReport]:
    """
    Enrich a DataFrame using GPT-4o.
    Adds new columns as defined in enrichment_config['output_columns'].
    """
    report = EnrichmentReport(total_rows=len(df))
    df = df.copy()

    input_col = enrichment_config.get("input_column")
    if not input_col or input_col not in df.columns:
        raise ValueError(f"input_column '{input_col}' not found in DataFrame columns: {list(df.columns)}")

    output_columns = enrichment_config.get("output_columns", {})
    if not output_columns:
        log.warning("No output_columns defined in enrichment config — skipping enrichment")
        return df, report

    batch_size = int(enrichment_config.get("batch_size", 10))
    model = os.getenv("MODEL_NAME", "gpt-4o")
    max_retries = int(os.getenv("MAX_RETRIES", 3))
    batch_delay = float(os.getenv("BATCH_DELAY_SECONDS", 1))

    system_prompt = _build_system_prompt(output_columns)
    client = OpenAI()

    # Initialize output columns with empty strings
    for col in output_columns:
        df[col] = ""

    texts = df[input_col].tolist()
    batches = [texts[i: i + batch_size] for i in range(0, len(texts), batch_size)]

    log.info(f"Enriching {len(texts)} rows in {len(batches)} batches of {batch_size}...")

    for batch_idx, batch in enumerate(tqdm(batches, desc="Enriching")):
        results = _call_gpt_batch(client, batch, system_prompt, model, max_retries)

        start_idx = batch_idx * batch_size
        for local_idx, result in enumerate(results):
            global_idx = start_idx + local_idx
            if global_idx >= len(df):
                break
            if result:
                for col in output_columns:
                    if col in result:
                        df.at[df.index[global_idx], col] = str(result[col])
                report.enriched_rows += 1
            else:
                report.failed_rows += 1

        if batch_delay > 0 and batch_idx < len(batches) - 1:
            time.sleep(batch_delay)

    report.columns_added = list(output_columns.keys())
    log.info(
        f"Enrichment complete: {report.enriched_rows} enriched, {report.failed_rows} failed"
    )
    return df, report

# IntelliSift — Automated Data Enrichment System

> Upload raw CSV or JSON data and receive a clean, enriched, AI-classified dataset with automated insights        .

![Python](https://img.shields.io/badge/Python-3.10+-blue) ![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o-green) ![Pandas](https://img.shields.io/badge/Data-Pandas-orange) ![License](https://img.shields.io/badge/License-MIT-yellow)

## The Problem

Data analysts spend 60–80% of their time cleaning and categorizing data by hand. IntelliSift automates the boring parts: validate and clean your raw data, then use GPT-4o to intelligently classify, tag, and enrich each row — in one pipeline run.

## Demo

<!-- Add your demo GIF or video link here -->
> 📹 Demo video coming soon

**What it does:**
```
Input:  messy_customer_feedback.csv  (500 rows, inconsistent values, missing fields)
Output: enriched_feedback.csv        (cleaned + sentiment + category + priority tags)
        pipeline_report.html         (summary stats, charts, issue log)
```

## Features

- Ingest CSV or JSON files (single file or a directory)
- Validation stage: flag missing values, wrong types, out-of-range numbers
- Cleaning stage: normalize text, fix date formats, deduplicate, fill nulls
- AI enrichment stage: GPT-4o classifies/tags/scores each row with configurable prompts
- Batch processing with rate-limit handling and automatic retries
- Outputs: enriched CSV + HTML summary report + SQLite database
- Fully configurable via YAML — no code changes needed for new datasets
- Dry-run mode for validation without API calls

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| LLM | OpenAI GPT-4o |
| Data | Pandas |
| Storage | SQLite (via sqlite3) |
| Report | Jinja2 HTML template |
| Config | YAML |

## Project Structure

```
IntelliSift/
├── pipeline.py             # Main pipeline orchestrator
├── stages/
│   ├── __init__.py
│   ├── ingest.py           # Load CSV/JSON into a DataFrame
│   ├── clean.py            # Validate and clean data
│   ├── enrich.py           # GPT-4o AI enrichment (batched)
│   └── export.py           # Write outputs: CSV, SQLite, HTML report
├── sample_data/
│   ├── customer_feedback.csv   # Example input file
│   └── pipeline_config.yaml    # Example pipeline config
├── output/                 # Generated outputs land here
├── requirements.txt
├── .env.example
└── README.md
```

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/ANSHRAMANI1/IntelliSift.git
cd IntelliSift
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Add your OPENAI_API_KEY
```

### 3. Run on sample data

```bash
# Full pipeline on sample data
python pipeline.py --config sample_data/pipeline_config.yaml

# Validation only (no AI calls, no cost)
python pipeline.py --config sample_data/pipeline_config.yaml --dry-run

# Specify a custom input file
python pipeline.py --config sample_data/pipeline_config.yaml --input path/to/your_data.csv
```

## How It Works

```
Input File (CSV/JSON)
        ↓
  [Stage 1: Ingest]     — Load file, detect schema, report shape
        ↓
  [Stage 2: Clean]      — Fix types, normalize text, drop/fill nulls
        ↓
  [Stage 3: Enrich]     — GPT-4o processes each row in batches
        ↓
  [Stage 4: Export]     — Enriched CSV + SQLite + HTML report
```

## Pipeline Config (YAML)

The pipeline is driven entirely by a YAML config file — no code changes needed.

```yaml
pipeline:
  name: "Customer Feedback Analysis"
  input_path: "sample_data/customer_feedback.csv"
  output_dir: "output"

cleaning:
  drop_duplicates: true
  required_columns: [customer_id, feedback_text]
  text_columns: [feedback_text]       # normalize whitespace, lowercase

enrichment:
  input_column: feedback_text         # column to send to GPT-4o
  batch_size: 10                      # rows per API call
  output_columns:
    sentiment:
      description: "Overall sentiment of the feedback"
      values: [positive, negative, neutral]
    category:
      description: "Main topic of the feedback"
      values: [product, shipping, customer_service, pricing, other]
    priority:
      description: "How urgently this needs a response"
      values: [high, medium, low]
    summary:
      description: "One-sentence summary of the feedback"
      type: text
```

## Sample Output

| customer_id | feedback_text | sentiment | category | priority | summary |
|-------------|--------------|-----------|----------|----------|---------|
| C001 | "Shipping took 3 weeks..." | negative | shipping | high | Customer frustrated by long shipping delay |
| C002 | "Love the product quality" | positive | product | low | Customer very satisfied with product quality |

## Use Cases

- **E-commerce** — Classify and prioritize customer reviews/feedback at scale
- **Sales teams** — Enrich lead lists with company type, industry, persona
- **HR** — Screen and categorize job applications, tag skills
- **Finance** — Classify transaction descriptions, tag anomalies
- **Content teams** — Categorize and tag articles, posts, or tickets

## Freelance Context

This was built as a portfolio project demonstrating production-ready AI-powered data pipelines. I customize IntelliSift for clients' specific data schemas, enrichment logic, and output destinations (BigQuery, Snowflake, Airtable, etc.).

**Want a custom data enrichment pipeline?** [Contact me on Upwork](https://www.upwork.com/freelancers/~0169dbb8a7f7cf38e6)

## License

MIT

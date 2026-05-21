# MovieLens PySpark Analysis

A take-home data engineering assignment demonstrating PySpark data ingestion, cleansing, transformation, and analytical exploration on the MovieLens small dataset.

## Project Structure

```
movie_lens_pyspark/
├── data/
│   └── raw/           # MovieLens CSV files (gitignored)
├── notebooks/
│   └── analysis.ipynb # Full analysis pipeline
├── src/
│   ├── spark_session.py    # Spark session factory
│   ├── ingestion.py        # Data loading with explicit schemas
│   ├── transformations.py # Silver & Gold layer transforms
│   └── analytics.py        # Analytical functions
├── reference/        # Problem statement and PySpark notes
├── pyproject.toml    # uv dependency management
└── README.md
```

## Setup Instructions

### Prerequisites
- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager
Use powershell: powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
Check: uv --version
uv 0.11.15 (3cffe97c2 2026-05-18 x86_64-pc-windows-msvc)

### Dataset Download
Download the MovieLens Latest Small dataset from:
kaggle mirror: https://www.kaggle.com/datasets/shubhammehta21/movie-lens-small-latest-dataset?resource=download

Extract and place the CSV files into `data/raw/`:
- movies.csv
- ratings.csv
- tags.csv
- links.csv

### Environment Setup

```bash
# Install dependencies with uv. This commands reads the pyproject.toml, creates a venv, downloads the dependencies and installs them.
# If have SSL certification error issue, try $env:UV_SYSTEM_CERTS = "1"
uv sync


# Run Jupyter notebook
uv run jupyter notebook
```

Navigate to `notebooks/analysis.ipynb` to run the full analysis.

## Key Findings

(To be populated after running analysis)

## Architecture

The pipeline follows a medallion architecture:

- **Bronze Layer**: Raw CSV ingestion with explicit schemas (avoids costly `inferSchema`)
- **Silver Layer**: Data cleansing (deduplication, timestamp normalization, genre explosion)
- **Gold Layer**: Aggregated business-ready tables (movie-level metrics by genre)

## Performance Considerations

- Explicit CSV schemas prevent extra data passes during schema inference
- Broadcast joins used for small dimension tables (movies)
- Narrow transformations preferred over wide where possible
- Spark runs in local mode for this laptop-scale dataset
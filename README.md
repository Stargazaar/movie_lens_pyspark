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

#### 1. Java (required by PySpark)
PySpark runs on the JVM, so Java must be installed. JDK 11, 17, or 21 all work.

Download from: https://adoptium.net/temurin/releases/?version=21&os=windows&arch=x64&package=jdk

Install the `.msi`, then verify:
```powershell
java -version
# Expected: openjdk version "21.x.x" ...
```

> **Note:** The notebook auto-detects your Java installation via `java -XshowSettings:property`, so no manual path configuration is needed after install.

#### 2. Python 3.10 or higher
Download from: https://www.python.org/downloads/

#### 3. uv package manager
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Verify: `uv --version`

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
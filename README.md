# GEO_ETL

GIS Data ETL Pipeline — PySpark-based extract-transform-load processes for geospatial data.

## Overview

GEO_ETL is the transformation layer in the GIS data pipeline. It reads raw CSV files produced by [GEO_Download](../GEO_Download), applies transformations and enrichment using PySpark, and outputs clean, analysis-ready datasets.

## Tech Stack

| Component        | Technology                    |
|------------------|-------------------------------|
| Language         | Python 3.11                   |
| Processing       | PySpark >= 3.5.0              |
| Data Validation  | Pydantic >= 2.0.0             |
| HTTP Client      | requests >= 2.31.0            |
| Testing          | pytest >= 7.0.0               |
| Environment      | Conda                         |

## Prerequisites

- **Conda** — for environment management
- **Java 11+** — required by PySpark
- **GEO_Download** — must be a sibling directory (source of raw CSVs)

## Setup

### 1. Create the Conda environment

```bash
conda env create -f environment.yml
conda activate geo_etl_311
```

### 2. Verify directory structure

```
GeographicData/
├── GEO_Download/       # Data downloader (source CSVs)
├── GEO_ETL/            # This project (transformations)
└── GEO_E2E/            # Orchestrator (schedules jobs)
```

## Project Structure

```
GEO_ETL/
├── environment.yml     # Conda environment definition
├── README.md
└── ...
```

## Usage

```bash
conda activate geo_etl_311
python run_etl.py <source_name>
```

## Testing

```bash
pytest test/
```

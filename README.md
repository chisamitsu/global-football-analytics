# 📊 Global Football Analytics
*A modular, end‑to‑end data engineering project built for learning, exploration, and fun.*

This project ingests football data from a public API, transforms it into a clean STAR schema, loads it into an analytical database, and prepares it for downstream analytics and visualizations. It follows modern data‑engineering patterns and emphasizes clarity, modularity, and reproducibility.

---

# 🚀 Project Architecture
The pipeline follows a clean, layered structure:
```text
Extract → Transform → Load → Analytics
```
Each layer is isolated, testable, and reusable.

---

# 🗂️ Folder Structure
```text
global-football-analytics/
│
├── src/
│   ├── extract/        # API extractors → raw data
│   ├── transform/      # STAR-schema transformations → clean data
│   ├── load/           # DuckDB Load Layer
│   └── analytics/      # (future) SQL queries, dashboards, notebooks
│
├── data/
│   ├── raw/            # Immutable API dumps (JSON, CSV)
│   ├── processed/      # Optional intermediate outputs (currently empty)
│   ├── clean/          # Final curated STAR-schema Parquet files
│   └── analytics.duckdb  # DuckDB analytical database (ignored by Git)
│
├── tests/
├── README.md
└── requirements.txt
```
---

# 🧩 Data Lake Zones

### 📁 raw/
Contains the exact files extracted from the API.  
No cleaning, no renaming, no schema changes.  
This layer is **immutable**.

### 📁 processed/
Optional intermediate zone.  
Useful for multi‑step transformations or debugging.  
Currently empty — the pipeline goes directly from raw → clean.

### 📁 clean/
The **Gold Layer**.  
Contains the final, curated STAR‑schema tables:
- `dim_team.parquet`
- `dim_player.parquet`
- `dim_venue.parquet`
- `dim_league.parquet`
- `fact_match.parquet`
- `fact_team_season.parquet`
- `fact_player_season.parquet`

These files are committed to Git so the project is fully reproducible.

### analytics.duckdb
The analytical database created by the Load Layer.  
Not committed to Git (binary, large, and fully reproducible).

---

# 🏗️ Extract Layer
Located in `src/extract/`.

Responsibilities:

- Connect to the football API  
- Download raw JSON files  
- Store them in `data/raw/`  
- Ensure idempotency (safe re‑runs)

To run the extractors:
```python
python -m src.extract.pipeline_extract
```

---

# 🔄 Transform Layer
Located in `src/transform/`.

Responsibilities:

- Read raw JSON files  
- Normalize and clean the data  
- Build a **STAR schema**  
- Write final Parquet files to `data/clean/`

To run the transforms:
```python
python -m src.transform.pipeline_transform
```

---

# 🗄️ Load Layer (DuckDB)
Located in `src/load/`.

Responsibilities:

- Create or open `data/analytics.duckdb`
- Load all Parquet files from `data/clean/`
- Create analytical tables inside DuckDB

Run the Load Layer:
```python
python -m src.load.pipeline_load
```
This will recreate the DuckDB database from the clean Parquet files.

---

# 📈 Analytics Layer (coming soon)
This layer will include:

- SQL queries for insights  
- DuckDB views  
- Jupyter notebooks  
- Visualizations and dashboards

---

# 🧪 Reproducibility
Install dependencies:
```python
pip install -r requirements.txt
```

Rebuild the entire database:
```python
python -m src.load.pipeline_load
```

The DuckDB file is not committed to Git — it is fully reproducible from the Parquet files.

---

# 🔒 Data Notes
- All data comes from a **public football API**  
- No personal or sensitive data is stored  
- Only Parquet files (clean layer) are committed  
- DuckDB is excluded via `.gitignore`

---

# 🧭 Future Improvements
- Add analytics notebooks  
- Add dashboards (Power BI, DuckDB, or Python)  
- Add support for multiple leagues and seasons  
- Add incremental ingestion  
- Add unit tests and CI/CD

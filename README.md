# causal-inference
A repo to contain my causal inference projects

# Repo Structure
causal-inference/
├── data/
│   ├── raw/               <-- (Untouched, messy CSVs go here)
│   └── processed/         <-- (Clean Parquet files go here)
├── notebooks/
├── src/
│   ├── config/            <-- (constants.py)
│   ├── data/              <-- (Ingestion functions: fetching, downloading)
│   ├── preprocessing/     <-- (Cleaning functions: formatting, joining, Arrow bridge)
│   ├── models/            <-- (SCM models)
│   └── utils/             <-- (Generic helpers: Spark sessions)
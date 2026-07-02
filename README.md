# causal-inference
A repo to contain my causal inference projects

# Repo Structure
```
causal-inference/
├── data/
│   ├── raw/                      <-- (Untouched, messy CSVs go here)
│   └── processed/                <-- (Clean files go here)
├── notebooks/
│   ├── dev/                      <-- (Scratch/development notebooks)
│   └── examples/                 <-- (Example/demo notebooks)
├── src/
│   ├── config/                   <-- (Constants, shared setup files)
│   ├── ingestion/                <-- (Ingestion functions: fetching, downloading)
│   ├── preprocessing/            <-- (Cleaning functions: formatting, joining, Arrow bridge)
│   ├── models/
│   │   └── synthetic_control/    <-- (SCM models)
│   └── utils/
│       ├── spark_utils/          <-- (Local Spark session setup)
│       └── scm_utils/            <-- (Model helpers)
└── tests/
    └── unit/                     <-- (Unit tests)
```
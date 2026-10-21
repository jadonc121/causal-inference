# Estimating the Causal Impact of RGGI on State CO2 Emissions

> **Status: in development.** Part 1 (data ingestion) is being built on the [`dev/data-ingestion`](https://github.com/jadonc121/causal-inference/tree/dev/data-ingestion) branch. This branch (`main`) holds the project scaffolding and will be updated when that work is merged.

A two-part project that builds modular, reusable data pipelines and then applies causal inference models to a real policy question: **What effect did the Regional Greenhouse Gas Initiative (RGGI), a regional cap on power-sector CO2 emissions that began on January 1, 2009, have on state-level emissions?**

## Why this project
Policy effects can't be seen by comparing "before" and "after", because emissions also move with weather, the economy, and energy markets. This project builds the data foundation and the models to separate a policy's effect from everything else that changed at the same time, and to show the methods I use in applied experimentation and measurement work.

## The plan

**Part 1: Data ingestion** *(in progress)*
Connect to four public data sources and retrieve the raw data needed for a state-by-month panel.

**Part 2: Causal models** *(planned)*
Estimate the effect of RGGI on monthly state CO2 emissions with at least two methods, and compare them.

### Status
- [x] FRED economic series pipeline (all 50 states)
- [x] NOAA heating and cooling degree-day pipeline (1997 onward)
- [ ] EIA energy data pipeline *(API exploration helpers done; data retrieval in progress)*
- [ ] EPA CAMPD emissions pipeline (the outcome variable)
- [ ] Preprocessing and panel assembly (cleaning, null and duplicate checks, joins)
- [ ] Unit tests
- [ ] Model 1: synthetic control (donor pool reweighted with entropy balancing)
- [ ] Model 2: difference-in-differences
- [ ] Possible extensions: matching, synthetic difference-in-differences (SDID)

## Data sources
| Source | Access | What it provides | Role | Status |
|---|---|---|---|---|
| EPA Clean Air Markets Program Data (CAMPD) | API | Emissions from power plants | Outcome: monthly state CO2 | Not started |
| EIA | API | Electricity generation and fuel mix | Covariates | In progress |
| NOAA Climate Prediction Center | File download and parsing | Monthly heating and cooling degree days by state | Covariates (weather) | Done |
| FRED (St. Louis Fed) | API | Unemployment rate, building permits, exports by state | Covariates (economy) | Done |

Other pollutants are deliberately excluded as covariates to avoid post-treatment bias.

## Models
The models answer the same question in different ways, so I can compare their estimates and check how sensitive the conclusion is to the method.

**Synthetic control (SCM).** For each treated state, the model builds a stand-in that never experienced the policy. In the pre-period (before January 2009), it reweights a donor pool of untreated states so that their weighted trajectory tracks the treated state's emissions and covariates as closely as possible. The weights are found with entropy balancing, a nonlinear optimization program that finds the optimal set of weights that satisfy the balance conditions. The weighted donor series is then forecast forward as a counterfactual: what emissions would have been without RGGI. The gap between the treated state's actual emissions and that counterfactual after 2009 is the estimated effect.

**Difference-in-differences (DiD).** DiD compares the change in emissions over time in the treated states (before vs. after RGGI) with the change over the same period in comparable untreated states. Subtracting the control group's change strips out shared influences such as weather, the economy, and national energy trends, leaving the change attributable to the policy. Its key assumption is *parallel trends*: without the policy, treated and control states would have moved in parallel. Checking that assumption in the pre-period is part of the analysis.

**Possible extensions:** matching models, and synthetic difference-in-differences (SDID).

## Design choices
- **Raw first.** Ingestion functions return only the relevant columns and deliberately do not clean the data. Null checks, de-duplication, and joins happen in a separate preprocessing stage, so the raw pull is always reproducible and every cleaning decision is visible.
- **Modular, importable code.** Each source has its own ingestion module with a clear entry point (`run_*_pipeline`), shared helpers, and a constants file.
- **Fast data collection.** Requests run in parallel thread pools with rate limiting and optional progress bars, and the NOAA pipeline probes each candidate URL first, since file names changed over the years.
- **Documented.** Every function has a docstring that covers arguments, returns, and errors.
- **Reproducible environment.** Python 3.13 managed with `uv` and a locked `pyproject.toml`, with a local Spark session for the larger transformations.
- **No data in git.** Downloaded files go to `data/raw/` and are git-ignored; run the pipelines to regenerate them.

## Repository layout (current, on `dev/data-ingestion`)
```
src/
├── config/          constants (states, months)
├── ingestion/       one module per data source + shared helpers
├── preprocessing/   cleaning and joins (planned)
├── models/
│   └── synthetic_control/   (planned)
└── utils/
    ├── spark_utils/ local Spark session setup
    └── scm_utils/   model helpers (planned)
data/raw, data/processed      generated data (git-ignored)
notebooks/examples            demo notebooks (planned)
tests/unit                    unit tests (planned)
```

## Getting started
```bash
git clone https://github.com/jadonc121/causal-inference.git
cd causal-inference
git checkout dev/data-ingestion
uv sync
```
The FRED and EIA APIs need free API keys; keep them in a local `.env` file (git-ignored), never in the code. Then call the pipeline for the source you want from a notebook or script, for example the NOAA pipeline:

```python
from src.utils.spark_utils.spark_session import get_spark_session
from src.ingestion.noaa_file_ingestion import run_noaa_ingestion_pipeline

spark = get_spark_session()
run_noaa_ingestion_pipeline(spark, file_path="data/raw/", dds_list=None, start_year=1997, end_year=2024)
```

## Skills demonstrated
Causal inference (synthetic control, difference-in-differences), API integration, data pipelines, parallel I/O, PySpark, pandas, Python packaging (`uv`, `pyproject.toml`), documentation, project structure.

## Next steps
Finish the EIA and CAMPD pipelines, build the preprocessing stage and panel, add unit tests, then fit and compare the models.

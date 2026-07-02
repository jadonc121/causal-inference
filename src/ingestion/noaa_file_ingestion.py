# Logic to collect degree days data from the NOAA website

import re
import numpy as np
import pandas as pd
import requests
import concurrent.futures

from datetime import datetime
from tqdm.auto import tqdm
from src.config.constants import US_STATE_NAMES, MONTH_NAMES
from src.ingestion.common import write_dataframe_to_csv

from pyspark.sql import DataFrame, SparkSession, functions as F

## Constants

CURRENT_YEAR = datetime.now().year

MONTH_NAME_FORMATTERS = {
    "full": lambda m: m,
    "full_lower": lambda m: str.lower(m),
    "abbrev": lambda m: m[:3],
    "abbrev_lower": lambda m: m[:3].lower()
}

NOAA_BASE_URL_SUBSTRING = "https://ftp.cpc.ncep.noaa.gov/htdocs/products/analysis_monitoring/cdus/degree_days/archives/"

COOLING_URL_SUBSTRING = "Cooling%20Degree%20Days/monthly%20cooling%20degree%20days%20state/"
HEATING_URL_SUBSTRING = "Heating%20degree%20Days/monthly%20states/"

NOAA_DDS_TYPES = ["cooling", "heating"]
NOAA_DDS_URL_SUBSTRING_BY_TYPE = dict(zip(NOAA_DDS_TYPES, [COOLING_URL_SUBSTRING, HEATING_URL_SUBSTRING]))

NOAA_URL_REPLACEMENTS = [
    {
        "year": 2001,
        "month": "July",
        "dds_type": "cooling",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: has comma
        "url_to_test": NOAA_BASE_URL_SUBSTRING + COOLING_URL_SUBSTRING + "2001/jul,%202001.txt",
    },
    {
        "year": 1998,
        "month": "July",
        "dds_type": "heating",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: has "hdd" string
        "url_to_test": NOAA_BASE_URL_SUBSTRING + HEATING_URL_SUBSTRING + "1998/jul%2098hdd.txt",
    },
    {
        "year": 1999,
        "month": "February",
        "dds_type": "heating",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: has short year "99"
        "url_to_test": NOAA_BASE_URL_SUBSTRING + HEATING_URL_SUBSTRING + "1999/feb%2099.txt",
    },
    {
        "year": 1999,
        "month": "December",
        "dds_type": "heating",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: has comma
        "url_to_test": NOAA_BASE_URL_SUBSTRING + HEATING_URL_SUBSTRING + "1999/dec,%201999.txt",
    },
    {
        "year": 2001,
        "month": "October",
        "dds_type": "heating",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: has comma
        "url_to_test": NOAA_BASE_URL_SUBSTRING + HEATING_URL_SUBSTRING + "2001/oct,%202001.txt",
    },
    {
        "year": 2002,
        "month": "November",
        "dds_type": "heating",
        "txt_suffix": None,
        "month_name_format": "exception", # exception: mislabeled as 2003
        "url_to_test": NOAA_BASE_URL_SUBSTRING + HEATING_URL_SUBSTRING + "2002/nov%202003.txt",
    },
]

## Functions

def _make_url_to_test(row: pd.Series) -> str:
    """Construct the candidate URL for a given degree-day file.

    Combines the NOAA base URL, the type-specific subdirectory, and the
    month/year token according to the formatting variant stored in the row.
    The `txt_suffix` flag controls whether a `.txt` extension is appended.

    Args:
        row: A row from the URL table with fields `dds_type`, `year`, `month`,
            `txt_suffix`, and `month_name_format`.

    Returns:
        The full candidate URL string to test against the NOAA FTP server.
    """
    url = NOAA_BASE_URL_SUBSTRING
    url += NOAA_DDS_URL_SUBSTRING_BY_TYPE[row["dds_type"]]
    url += f"{row['year']}/{MONTH_NAME_FORMATTERS[row['month_name_format']](row['month'])}"
    
    if row["txt_suffix"]:
        url_txt_suffix = ".txt"
    else:
        url_txt_suffix = ""

    url += f"%20{row["year"]}{url_txt_suffix}"

    return url

def _replace_exceptional_urls(url_df: pd.DataFrame) -> pd.DataFrame:
    """Replace rows whose URLs have non-standard formatting with hardcoded overrides.

    Looks up each row in `NOAA_URL_REPLACEMENTS` by `(dds_type, year, month)`
    and removes the matching generated rows, then appends the hand-crafted
    replacements. This handles edge cases like comma-separated month names,
    short year tokens, or mislabeled files on the NOAA FTP server.

    Args:
        url_df: The generated URL table from `_build_url_table`.

    Returns:
        A new DataFrame with exceptional-format rows substituted by their
        correct hardcoded counterparts. Row order is not preserved.
    """

    replacement_urls_df = pd.DataFrame(NOAA_URL_REPLACEMENTS)

    join_columns = ["dds_type", "year", "month"]

    filtered_urls_df = url_df.merge(
        replacement_urls_df[join_columns],
        on=join_columns,
        how="left_anti"
    )

    return pd.concat([filtered_urls_df, replacement_urls_df], ignore_index=True)

def _build_url_table(
        dds_list: list[str] = None,
        start_year: int = 1997,
        end_year: int = CURRENT_YEAR
) -> pd.DataFrame:
    """Build the full table of candidate NOAA degree-day file URLs.

    Generates every combination of degree-day type, year, month, `.txt` suffix
    flag, and month-name format, then constructs a URL for each. Exceptional
    cases are swapped in via `_replace_exceptional_urls`, and duplicate rows
    that arise from month abbreviations that are identical across format
    variants (e.g. "May") are dropped.

    Args:
        dds_list: Degree-day types to include. Must be a subset of
            `NOAA_DDS_TYPES` (`["cooling", "heating"]`). Defaults to both
            types when `None`.
        start_year: First year of data to include. Must be 1997 or later
            (no NOAA archive data exists before 1997). Defaults to 1997.
        end_year: Last year of data to include (inclusive). Must be the
            current year or earlier. Defaults to the current year.

    Returns:
        A DataFrame with columns `dds_type`, `year`, `month`, `txt_suffix`,
        `month_name_format`, and `url_to_test`, one row per unique candidate
        URL.

    Raises:
        AssertionError: If `dds_list` contains values outside `NOAA_DDS_TYPES`,
            `start_year` is before 1997, `end_year` exceeds the current year,
            or `start_year` is greater than `end_year`.
    """
    if dds_list is None:
        dds_list = NOAA_DDS_TYPES
    else:
        dds_assert_msg = f"dds_list elements must be in {NOAA_DDS_TYPES}"
        assert set(dds_list).issubset(NOAA_DDS_TYPES), dds_assert_msg

    assert start_year > 1996, f"start_year must be 1997 or later (no data exists prior to 1997), received: {start_year}"
    assert end_year <= CURRENT_YEAR, f"end_year must be current year or earlier, received: {end_year}"
    assert start_year <= end_year, f"start_year must be less than or equal to end_year"

    url_df = pd.DataFrame([
        [dds_type, year, month, txt, month_name_format]
        for dds_type in dds_list
        for year in np.arange(start_year, end_year + 1)
        for month in MONTH_NAMES
        for txt in [True, False]
        for month_name_format in MONTH_NAME_FORMATTERS.keys()
        ],
        columns=["dds_type", "year", "month", "txt_suffix", "month_name_format"]
    )

    url_df["url_to_test"] = url_df.apply(_make_url_to_test, axis=1)

    # Replace URLs with exceptional formatting
    url_df = _replace_exceptional_urls(url_df)

    # Remove duplicate rows for May
    dedupe_cols = url_df.columns.drop("month_name_format")
    
    return url_df.drop_duplicates(subset=dedupe_cols).reset_index(drop=True)

def _check_url_exists(url: str) -> bool:
    """Return whether a URL responds with HTTP 200.

    Issues a HEAD request to avoid downloading the file body. Any network
    error or non-200 response is treated as non-existent.

    Args:
        url: The URL to probe.

    Returns:
        `True` if the server returns a 200 status code, `False` otherwise.
    """
    try:
        response = requests.head(url, timeout=5)
        
        if response.status_code == 200:
            return True
        else:
            return False
        
    except requests.RequestException as e:
        return False

def _run_parallel_url_check_requests(
        url_df: pd.DataFrame,
        progress_bar: bool = False
) -> pd.DataFrame:
    """Check all URLs in the table for existence using a thread pool.

    Dispatches `_check_url_exists` concurrently across up to 50 threads and
    writes the results into a new `url_exists` column on the input DataFrame.

    Args:
        url_df: DataFrame containing a `url_to_test` column with candidate URLs.
        progress_bar: Whether to display a tqdm progress bar during execution.
            Defaults to `False`.

    Returns:
        The same DataFrame with a boolean `url_exists` column appended.
    """
    urls = url_df["url_to_test"].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        if progress_bar:
            progress_bar_desc = "Checking URLs exist in parallel"
            results = list(tqdm(executor.map(_check_url_exists, urls), total=len(urls), desc=progress_bar_desc))
        else:
            results = list(executor.map(_check_url_exists, urls))

    url_df["url_exists"] = results

    return url_df

def _get_url_text(url: str) -> str | None:
    """Fetch the raw text content of a URL.

    Issues a GET request and returns the response body as a string. Any
    network error or non-200 response returns `None`.

    Args:
        url: The URL to fetch.

    Returns:
        The response body as a string, or `None` if the request fails or the
        server returns a non-200 status code.
    """
    try:
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
            return response.text
        else:
            return None
        
    except requests.RequestException as e:
        return None
    
def _run_parallel_url_text_requests(
        url_df: pd.DataFrame,
        progress_bar: bool = False
) -> pd.DataFrame:
    """Fetch the text content of all URLs in the table using a thread pool.

    Dispatches `_get_url_text` concurrently across up to 50 threads and
    writes the results into a new `url_text` column on the input DataFrame.

    Args:
        url_df: DataFrame containing a `url_to_test` column with URLs to fetch.
            Typically the subset of rows where `url_exists` is `True`.
        progress_bar: Whether to display a tqdm progress bar during execution.
            Defaults to `False`.

    Returns:
        The same DataFrame with a `url_text` column appended containing the
        raw response body for each URL, or `None` where the request failed.
    """
    urls = url_df["url_to_test"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        if progress_bar:
            progress_bar_desc = "Requsting URL texts in parallel"
            results = list(tqdm(executor.map(_get_url_text, urls), total=len(urls), desc=progress_bar_desc))
        else:
            results = list(executor.map(_get_url_text, urls))

    url_df["url_text"] = results

    return url_df

def _extract_state_values_from_text(row: pd.Series) -> list[list[str]]:
    """Parse the degree-day value for each US state from a NOAA text file body.

    Scans the raw fixed-width text for each state name in `US_STATE_NAMES`
    using an uppercase regex match followed by a numeric capture group. Values
    are cast to `float` strings to normalize formatting; states with no match
    or a non-numeric capture are recorded as `None`.

    Args:
        row: A row from the URL text DataFrame. Must contain a `url_text`
            field with the raw text body of a NOAA degree-day file.

    Returns:
        A list of `[state_name, value]` pairs — one per state in
        `US_STATE_NAMES` — where `value` is a string representation of a
        float (e.g. `"1234.0"`) or `None` if no value was found.
    """
    text = row["url_text"]

    state_values = []

    for state in US_STATE_NAMES:
        pattern = rf"{re.escape(state.upper())}\s+(\d+(?:\.\d+)?)"
        match = re.search(pattern, text) 

        if match:
            try:
                value = str(float(match.group(1)))
            except ValueError as e:
                value = None
        else:
            value = None

        state_values.append([state, value])

    return state_values 

def _explode_state_values(condensed_df: DataFrame) -> DataFrame:
    """Explode the nested `state_values` column into one row per state.

    Each element of `state_values` is a two-element array of `[state, total]`.
    This function explodes that array, then promotes the two sub-fields into
    top-level `state` and `total` columns.

    Args:
        condensed_df: A Spark DataFrame with a `state_values` column whose
            entries are arrays of `[state_name, value]` pairs, as produced
            by applying `_extract_state_values_from_text` across rows.

    Returns:
        A Spark DataFrame with `state` and `total` columns added, one row per
        state per original row. The intermediate `state_values_exploded` column
        is retained in the output.
    """
    exploded_df = (
        condensed_df
        .withColumn(
            "state_values_exploded",
            F.explode(F.col("state_values"))
        )
        .withColumns(
            dict(zip(
                ["state", "total"],
                [F.col("state_values_exploded")[0], F.col("state_values_exploded")[1]]
            ))
        )
    )

    return exploded_df 

def run_noaa_ingestion_pipeline(
        spark: SparkSession,
        file_path: str,
        dds_list: list[str] | None, 
        start_year: int, 
        end_year: int,
        progress_bar: bool = False,
        write_url_df: bool = False
) -> None:
    """Fetch NOAA heating and cooling degree-day data and write it to CSV.

    Orchestrates the full ingestion pipeline: builds the candidate URL table,
    probes each URL for existence in parallel, fetches the text content of
    confirmed URLs, parses per-state values from each file, pivots heating and
    cooling into separate columns, and writes the result to disk.

    `file_path` must be a directory prefix that ends with a `/` — the function
    appends its own filenames. Pass `"data/raw/"` (repo root-relative) to match
    the standard data layout. Output files written:

    - `noaa_dds_covariates.csv` — the final `(state, year, month, cooling,
      heating)` panel.
    - `noaa_dds_urls.csv` — URL metadata with existence and text columns
      (only written when `write_url_df=True`).

    Args:
        spark: Active SparkSession used for the pivot and write steps.
        file_path: Directory prefix to prepend to each output filename.
            Must end with `/` (e.g. `"data/raw/"`).
        dds_list: Degree-day types to include. Defaults to both types when
            `None`. See `_build_url_table` for valid values.
        start_year: First year of data to fetch. Must be 1997 or later.
        end_year: Last year of data to fetch (inclusive).
        progress_bar: Whether to display tqdm progress bars during parallel
            HTTP requests. Defaults to `False`.
        write_url_df: Whether to write the URL metadata table alongside the
            main output. Defaults to `False`.
    """

    # Build URL df - figure out the inputs here
    base_urls_df = _build_url_table(dds_list=dds_list, start_year=start_year, end_year=end_year)

    # Run parallel job to get URLS
    checked_urls_df = _run_parallel_url_check_requests(url_df=base_urls_df, progress_bar=progress_bar)

    # Filter to real URLs
    real_urls_df = checked_urls_df[checked_urls_df["url_exists"] == True]

    # Run parallel URL text requests
    urls_with_text_df = _run_parallel_url_text_requests(url_df=real_urls_df, progress_bar=progress_bar)

    # Parse texts responses
    urls_with_text_df["state_values"] = urls_with_text_df.apply(_extract_state_values_from_text, axis=1)

    # Explode df 
    state_values_condensed_df = spark.createDataFrame(urls_with_text_df)
    state_values_exploded_df = _explode_state_values(state_values_condensed_df)

    # Pivot dds cols
    pivoted_df = (
        state_values_exploded_df
        .groupBy("state", "year", "month")
        .pivot("dds_type")
        .agg(F.first("total"))
        .orderBy("state", "year", F.to_date(F.col("month"), "MMMM"))
    )

    # Write function
    dds_df_file_path = file_path + "noaa_dds_covariates.csv"
    write_dataframe_to_csv(df=pivoted_df, file_path=dds_df_file_path)

    if write_url_df:
        url_metedata_df = checked_urls_df.merge(
            real_urls_df[["url_to_test", "url_text", "state_values"]],
            on="url_to_test",
            how="left"
        )

        url_df_file_path = file_path + "noaa_dds_urls.csv"
        write_dataframe_to_csv(df=url_metedata_df, file_path=url_df_file_path)
    
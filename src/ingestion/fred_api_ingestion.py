import pandas as pd
import time
import concurrent.futures


from fredapi import Fred
from tqdm.auto import tqdm
from functools import reduce, partial
from src.config.constants import US_STATE_ABBREVS
from src.ingestion.common import write_dataframe_to_csv

## Constants

RATE_LIMIT_PAUSE_AMOUNT = 2 # in seconds
N_MAX_WORKERS = 5 # for multithreading client requests

# Column definitions
FRED_DATA_COLUMN_DEFINITIONS = {
    "BPPRIV": "New private housing units authorized by building permit, not seasonally adjusted",
    "URN": "Unemployment rate, not seasonally adjusted",
    "EXPTOT": "Export of goods, not seasonally adjusted"
}

FRED_DATA_COLUMN_UNITS = {
    "BPPRIV": "Housing units",
    "URN": "Percent",
    "EXPTOT": "Millions of dollars, USD"
}

# Series strings

SERIES_SUFFIXES = ["BPPRIV", "URN"]
SERIES_PREFIXES = ["EXPTOT"]

SERIES_NAMES_BY_TYPE = dict(
    [(prefix, "prefix") for prefix in SERIES_PREFIXES] +
    [(suffix, "suffix") for suffix in SERIES_SUFFIXES]
)

BASE_COLUMNS = ["state", "year", "month"]

## Functions

def _create_empty_series_df(
    series_by_name: dict,
    state_abbrevs: list
) -> pd.DataFrame:
    """Build the cross product of series names and state abbreviations.

    Args:
        series_by_name: Mapping of series name to series type; only its keys
            are used here.
        state_abbrevs: State abbreviations to cross with each series name.

    Returns:
        A DataFrame with one row per (state, series_name) pair, containing
        "state" and "series_name" columns.
    """

    series_names = series_by_name.keys()
    series_names_df = pd.DataFrame(series_names, columns=["series_name"])
    states_df = pd.DataFrame(state_abbrevs, columns=["state"])

    return states_df.join(series_names_df, how="cross")

def _build_series_id(
    state: str,
    series_name: str,
    series_type: str
) -> str:
    """Construct a FRED series ID by combining a state abbreviation with a series name.

    Args:
        state: Uppercase two-letter state abbreviation (e.g. "CA"). Must be
            a member of US_STATE_ABBREVS.
        series_name: Base FRED series code (e.g. "URN", "EXPTOT").
        series_type: Where the state abbreviation goes relative to
            series_name. Must be "prefix" (series_name + state) or "suffix"
            (state + series_name).

    Returns:
        The combined FRED series ID string.

    Raises:
        AssertionError: If state is not a valid uppercase state abbreviation.
        ValueError: If series_type is not "prefix" or "suffix".
    """
    state_assert_msg = f"state must be an uppercase state abbreviation. Received: {state}"
    assert state in US_STATE_ABBREVS, state_assert_msg

    if series_type == "prefix":
        return f"{series_name}{state}"
    elif series_type == "suffix":
        return f"{state}{series_name}"
    else:
        msg = 'Series type was not set as "prefix" or "suffix"'
        raise ValueError(msg)
    
def _apply_series_id(
        row: pd.Series,
        series_by_name_dict: dict
) -> str:
    """Derive the FRED series ID for a single row via _build_series_id.

    Args:
        row: A pandas Series with "state" and "series_name" fields, typically
            one row of the empty series DataFrame produced by
            _create_empty_series_df.
        series_by_name_dict: Mapping of series name to series type ("prefix"
            or "suffix"), used to look up how row["series_name"] should be
            combined with row["state"].

    Returns:
        The FRED series ID string for the row.
    """
    return _build_series_id(
        row["state"],
        row["series_name"],
        series_by_name_dict[row["series_name"]]
    )
    
def _make_fred_client_request(
    series_id: str,
    fred_client: Fred
) -> pd.Series | None:
    """Fetch a single series from the FRED API and name it after its series ID.

    Args:
        series_id: FRED series ID to request (e.g. "CAURN").
        fred_client: Authenticated fredapi.Fred client used to make the request.

    Returns:
        The requested series as a pandas Series named series_id.

    Raises:
        Exception: Re-raises any exception encountered while fetching the
            series from the FRED API.

    TODO: Update expection handling to specific checks
    TODO: Determine what do return if failure: None or empty series?
    """

    try:
        data = fred_client.get_series(series_id)
        data.name = series_id
    except Exception as e:
        data = None
        raise e

    return data

def _format_series_data(
    series: pd.Series,
    series_name: str,
    state: str 
) -> pd.DataFrame:
    """Reshape a raw FRED series into a per-state, per-month row format.

    Args:
        series: Raw FRED series data, indexed by date.
        series_name: Column name to assign to the series' values.
        state: State abbreviation to attach to every row.

    Returns:
        A DataFrame with columns [series_name, "month", "year", "state"],
        one row per date in the input series.
    """

    series.name = series_name
    series_df = series.to_frame().reset_index()

    series_df = series_df.rename(columns={"index": "date"})
    series_df["date"] = pd.to_datetime(series_df["date"])
    series_df["month"] = series_df["date"].dt.month
    series_df["year"] = series_df["date"].dt.year
    series_df["state"] = state

    return series_df.drop(columns=["date"])

def _get_fred_series(
    row: pd.Series,
    fred_client: Fred,
    rate_limit: float
) -> pd.DataFrame:
    """Fetch and format one series row, pausing afterward to respect FRED's rate limit.

    Args:
        row: A pandas Series with "state", "series_name", and "series_id"
            fields, typically one row of the series DataFrame.
        fred_client: Authenticated fredapi.Fred client used to make the request.
        rate_limit: Seconds to sleep after the request completes.

    Returns:
        The formatted series data for the row, as produced by
        _format_series_data.
    """
    row_column_names = ["state", "series_name", "series_id"]
    for column_name in row_column_names:
        row_assert_msg = f"Expected {column_name} field in row series object"
        assert column_name in row.index, row_assert_msg

    row_state = row["state"]
    row_series_name = row["series_name"]
    row_series_id = row["series_id"]

    series_data = _make_fred_client_request(
        series_id=row_series_id,
        fred_client=fred_client
    )
    
    time.sleep(rate_limit)

    return _format_series_data(
        series=series_data,
        series_name=row_series_name,
        state=row_state
    )

def _run_parallel_series_requests(
    series_df: pd.DataFrame,
    fred_client: Fred,
    n_max_workers: int,
    rate_limit: float,
    progress_bar: bool = False
):
    """Fetch each row's FRED series concurrently using a thread pool.

    Args:
        series_df: DataFrame of series rows to fetch, each with "state",
            "series_name", and "series_id" columns.
        fred_client: Authenticated fredapi.Fred client used to make requests.
        n_max_workers: Maximum number of worker threads in the pool.
        rate_limit: Seconds each worker sleeps after its request, passed
            through to _get_fred_series.
        progress_bar: Whether to display a tqdm progress bar while requests
            are in flight. Defaults to False.

    Returns:
        series_df with a new "series_data" column holding each row's
        formatted series DataFrame.
    """

    series_rows = [row for _, row in series_df.iterrows()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_max_workers) as executor:
        executor_mapping = executor.map(partial(_get_fred_series, fred_client=fred_client, rate_limit=rate_limit), series_rows)

        if progress_bar:
            progress_bar_desc = "Requesting FRED URLs in parallel"
            series_data_dfs = list(tqdm(executor_mapping, total=len(series_rows), desc=progress_bar_desc))
        else:
            series_data_dfs = list(executor_mapping)

    series_df["series_data"] = series_data_dfs

    return series_df

def _merge_two_dfs(
        df1: pd.DataFrame,
        df2: pd.DataFrame
) -> pd.DataFrame:
    """Outer-merge two series DataFrames on the shared state/year/month key.

    Args:
        df1: First DataFrame to merge.
        df2: Second DataFrame to merge.

    Returns:
        The outer join of df1 and df2 on BASE_COLUMNS ("state", "year",
        "month").
    """
    return pd.merge(df1, df2, on=BASE_COLUMNS, how="outer")

def _rollup_series_df(
    long_series_df: pd.DataFrame,
    base_columns: list[str],
    series_by_name: dict
) -> pd.DataFrame:
    """Collapse the long, per-series rows into one wide row per state/year/month.

    For each state, merges that state's individual series DataFrames
    together on base_columns and keeps only the base and series columns.

    Args:
        long_series_df: DataFrame with one row per (state, series_name),
            including a "series_data" column of per-series DataFrames, as
            produced by _run_parallel_series_requests.
        base_columns: Key columns to merge and select on (e.g. ["state",
            "year", "month"]).
        series_by_name: Mapping of series name to series type; only its keys
            are used to select the series value columns.

    Returns:
        A single DataFrame with one row per state/year/month and one column
        per series name, concatenated across all states.
    """
    state_dfs = []
    
    for state, sub_df in long_series_df.groupby("state"):
        series_dfs = sub_df["series_data"].tolist()

        cols = base_columns + list(series_by_name.keys())
        state_df = reduce(_merge_two_dfs, series_dfs)[cols]
        state_dfs.append(state_df)

    return pd.concat(state_dfs, ignore_index=True)

def run_fred_ingestion_pipeline2(
    series_by_name: dict,
    states_abbrevs: list[str],
    base_columns: list[str],
    fred_client: Fred,
    file_path: str,
    rate_limit: float = RATE_LIMIT_PAUSE_AMOUNT,
    n_max_workers: int = N_MAX_WORKERS,
    progress_bar: bool = False
) -> None:
    """Fetch FRED series for every state and write the assembled panel to CSV.

    Builds the (state, series_name) grid, derives each row's FRED series ID,
    fetches the data in parallel, rolls it up into one wide row per
    state/year/month, and writes the result to file_path.

    Args:
        series_by_name: Mapping of FRED series name to series type ("prefix"
            or "suffix"), e.g. FRED_DATA_COLUMN_DEFINITIONS' keys.
        states_abbrevs: State abbreviations to fetch series for.
        base_columns: Key columns used to merge and select the final panel
            (e.g. BASE_COLUMNS).
        fred_client: Authenticated fredapi.Fred client used to make requests.
        file_path: Destination path for the written CSV.
        rate_limit: Seconds to sleep between each series request. Defaults
            to RATE_LIMIT_PAUSE_AMOUNT.
        n_max_workers: Maximum number of worker threads used for parallel
            requests. Defaults to N_MAX_WORKERS.
        progress_bar: Whether to display a tqdm progress bar while requests
            are in flight. Defaults to False.
    """

    # Create empty dataframe to fill
    empty_df = _create_empty_series_df(series_by_name=series_by_name, state_abbrevs=states_abbrevs)

    # Create series IDs
    empty_df["series_id"] = empty_df.apply(_apply_series_id, axis=1, args=(series_by_name,))

    # Get series data
    series_df = _run_parallel_series_requests(
        series_df=empty_df, 
        fred_client=fred_client,
        n_max_workers=n_max_workers,
        rate_limit=rate_limit,
        progress_bar=progress_bar
    )

    # Rollup data into single frame
    final_df = _rollup_series_df(series_df, base_columns=base_columns, series_by_name=series_by_name)

    # Write data to file
    write_dataframe_to_csv(df=final_df, file_path=file_path)




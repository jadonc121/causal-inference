import pandas as pd
import time
import concurrent.futures
import functools

from fredapi import Fred
from tqdm.auto import tqdm
from src.config.constants import US_STATE_ABBREVS
from src.ingestion.common import write_dataframe_to_csv

## Constants

RATE_LIMIT_PAUSE_AMOUNT = 0.5 # in seconds

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

## Functions - DEPRECATE

def _get_fred_data(
    fred_client: Fred,
    states_list: list[str],
    suffix_list: list[str],
    prefix_list: list[str],
    base_columns: list[str],
    rate_limit: float
) -> pd.DataFrame:
    """Fetch FRED series for every state and assemble a (state, year, month) panel.

    For each state, pulls the suffix-keyed series (series ID built as
    state + suffix) and prefix-keyed series (series ID built as prefix +
    state), reshapes each to monthly rows, and outer-merges them onto a
    shared base grain so missing observations across sources are preserved.
    Sleeps between API calls to stay within the FRED rate limit.

    Args:
        fred_client: Authenticated FRED client used to fetch series.
        states_list: State abbreviations to iterate over, forming the panel rows.
        suffix_list: Series codes appended to each state abbreviation (e.g. URN).
        prefix_list: Series codes prepended to each state abbreviation (e.g. EXPTOT).
        base_columns: Join keys defining the panel grain (state, year, month).
        rate_limit: Seconds to pause after each series fetch.

    Returns:
        A DataFrame of all states stacked, with the base columns plus one
        feature column per series code.
    """

    all_fred_data = []

    for state in states_list:
        state_df = pd.DataFrame([], columns=base_columns)

        # Suffix series
        for suffix in suffix_list:
            series_id = f"{state}{suffix}"
            data = fred_client.get_series(series_id)

            df = pd.DataFrame(data, columns=[suffix]).reset_index()

            df.rename(columns={"index": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df["month"] = df["date"].dt.month
            df["year"] = df["date"].dt.year
            df.drop(columns=["date"], inplace=True)
            df["state"] = state

            state_df = pd.merge(state_df, df, on=base_columns, how="outer")

            time.sleep(rate_limit)

        # Prefix series
        for prefix in prefix_list:
            series_id = f"{prefix}{state}"
            data = fred_client.get_series(series_id)

            df = pd.DataFrame(data, columns=[prefix]).reset_index()

            df.rename(columns={"index": "date"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df["month"] = df["date"].dt.month
            df["year"] = df["date"].dt.year
            df.drop(columns=["date"], inplace=True)
            df["state"] = state

            state_df = pd.merge(state_df, df, on=base_columns, how="outer")

            time.sleep(rate_limit)

        all_fred_data.append(state_df)

    return pd.concat(all_fred_data, ignore_index=False)

def run_fred_ingestion_pipeline(fred_client: Fred, file_path: str) -> None:
    """Run the full FRED ingestion: fetch every state's series and write the panel.

    Composes get_fred_data and write_fred_data using the module-level series
    and column configuration, producing a single CSV at the output file path.

    Args:
        fred_client: Authenticated FRED client used to fetch series.
        file_path: Destination path for the written CSV panel.
    """

    fred_data_df = _get_fred_data(
        fred_client=fred_client,
        states_list=US_STATE_ABBREVS,
        suffix_list=SERIES_SUFFIXES,
        prefix_list=SERIES_PREFIXES,
        base_columns=BASE_COLUMNS,
        rate_limit=RATE_LIMIT_PAUSE_AMOUNT
    )

    write_dataframe_to_csv(df=fred_data_df, file_path=file_path)

## Functions - REFACTORS

def _build_series_id(
    state: str,
    series_name: str,
    series_type: str
) -> str:
    """
    TODO: Add docstring
    TODO: REDO using new format: pull this info from df col names
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
    """
    TODO: Add docstring
    """
    return _build_series_id(
        row["state"],
        row["series_name"],
        series_by_name_dict[row["series_name"]]
    )
    
def _create_empty_series_df(
    series_by_name_dict: dict,
    state_abbrevs_list: list
) -> pd.DataFrame:
    """
    TODO: Add docstring
    """

    series_names = series_by_name_dict.keys()
    series_names_df = pd.DataFrame(series_names, columns=["series_name"])
    states_df = pd.DataFrame(state_abbrevs_list, columns=["state"])

    return states_df.join(series_names_df, how="cross")

def _make_fred_client_request(
    series_id: str,
    fred_client: Fred
) -> pd.Series:
    """
    TODO: Add docstring
    TODO: Add smarter, specific exception catching
    TODO: Decide what to return if error: None object? empty series?
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
    """
    TODO: Add docstring
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
    fred_client: Fred
) -> pd.DataFrame:
    """
    TODO: Add docstring
    TODO: Condense return object to be the result of the _format func
    """
    
    row_state = row["state"]
    row_series_name = row["series_name"]
    row_series_id = row["series_id"]

    series_data = _make_fred_client_request(
        series_id=row_series_id,
        fred_client=fred_client
    )

    formatted_series_data = _format_series_data(
        series=series_data,
        series_name=row_series_name,
        state=row_state
    )

    return formatted_series_data

def _run_parallel_series_requests(
        states_list: list[str],
        progress_bar: bool = False
):
    """
    TODO: Add docstring
    TODO: Determine how to wrap the executor mapping: wrap in list() or other?
    """

    state_dfs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        if progress_bar:
            progress_bar_desc = "Requesting FRED URLs in parallel"
            # results = list(tqdm(executor.map(_check_url_exists, urls), total=len(urls), desc=progress_bar_desc))
            state_dfs.append(tqdm(executor.map(_get_fred_series, states_list), total=len(states_list), desc=progress_bar_desc))
        else:
            # results = list(executor.map(_check_url_exists, urls))
            state_dfs.append(executor.map(_get_fred_series, states_list))

    # return list of state dfs OR merged dfs

def _merge_two_dfs(
        df1: pd.DataFrame,
        df2: pd.DataFrame
) -> pd.DataFrame:
    """
    TODO: Add docstring
    """
    return pd.merge(df1, df2, on=BASE_COLUMNS, how="outer")

def _rollup_series_df(
    long_series_df: pd.DataFrame,
    base_columns: list[str],
    series_by_name: dict
) -> pd.DataFrame:
    """
    TODO: Add docstring
    """
    state_dfs = []
    
    for state, sub_df in long_series_df.groupby("state"):
        series_dfs = sub_df["series_data"].tolist()

        cols = base_columns + list(series_by_name.keys())
        state_df = functools.reduce(_merge_two_dfs, series_dfs)[cols]
        state_dfs.append(state_df)

    return pd.concat(state_dfs, ignore_index=True)

def run_fred_ingestion_pipeline(
    series_by_name: dict,
    states_abbrevs: list[str],
    fred_client: Fred,
    base_columns: list[str],
) -> None:
    """
    TODO: Add docstring
    TODO: Add write functionality
    TODO: Update with parallelization logic
    """

    # Create empty dataframe to fill
    series_df = _create_empty_series_df(series_by_name=series_by_name, state_abbrevs_list=states_abbrevs)

    # Create series IDs
    series_df["series_id"] = series_df.apply(_apply_series_id, axis=1, args=(series_by_name,))

    # Get series data - parallelize here!
    series_df["series_data"] = series_df.apply(_get_fred_series, axis=1, args=(fred_client,))

    # Rollup data into single frame
    final_df = _rollup_series_df(series_df, base_columns=base_columns, series_by_name=series_by_name)

    # Write data to file
    final_df.count() # replace with write func




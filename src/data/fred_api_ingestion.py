import pandas as pd
import time

from fredapi import Fred
from src.config.constants import US_STATE_ABBREV_LIST

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

BASE_COLUMNS = ["state", "year", "month"]

## Functions

def get_fred_data(
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

def write_fred_data(df: pd.DataFrame, file_path: str) -> None:
    """Write the assembled FRED panel to a CSV file.

    This will overwrite any file currently stored at the given file_path.

    Args:
        df: The FRED panel to persist.
        file_path: Destination path for the CSV.

    Raises:
        OSError: If the file cannot be written; annotated with the target path
            and re-raised.
    """

    try:
        df.to_csv(file_path, index=False)
    except OSError as e:
        e.add_note(f"Failed to write file to CSV with file path: {file_path}")
        raise 

def run_fred_ingestion_pipeline(fred_client: Fred, output_path: str) -> None:
    """Run the full FRED ingestion: fetch every state's series and write the panel.

    Composes get_fred_data and write_fred_data using the module-level series
    and column configuration, producing a single CSV at the output path.

    Args:
        fred_client: Authenticated FRED client used to fetch series.
        output_path: Destination path for the written CSV panel.
    """

    fred_data_df = get_fred_data(
        fred_client=fred_client,
        states_list=US_STATE_ABBREV_LIST,
        suffix_list=SERIES_SUFFIXES,
        prefix_list=SERIES_PREFIXES,
        base_columns=BASE_COLUMNS,
        rate_limit=RATE_LIMIT_PAUSE_AMOUNT
    )

    write_fred_data(df=fred_data_df, file_path=output_path)



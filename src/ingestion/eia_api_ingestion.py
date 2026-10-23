import pandas as pd
import numpy as np
import requests

from IPython.display import HTML
from typing import Optional, List, Tuple
from src.ingestion.common import write_dataframe_to_csv

## CONSTANTS

EIA_BASE_URL = "https://api.eia.gov/v2/"

N_MAX_ROWS_EIA_API = 5000

## FUNCTIONS
def _build_view_url(
        api_key: str,
        route_ids: Optional[List[str]] = None,
        view_data_size: bool = False,
        view_facet_metadata: bool = False,
        facet_id_to_view: str = None
    ) -> str:
    """
    Create a URL to pass to the EIA API from the given API key and route IDs list.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage. Default is None.
            An empty or None route_ids list will return the base EIA API URL.
        view_data_size: Whether to append the "/data" segment to view the dataset's size metadata.
            Defaults to False.
        view_facet_metadata: Whether to append the "/facet/{facet_id_to_view}" segment to view
            metadata for a specific facet. Defaults to False.
        facet_id_to_view: Facet ID to view metadata for when view_facet_metadata is True. Required
            in that case; ignored otherwise. Defaults to None.

    Returns:
        The EIA URL as a string.

    Raises:
        ValueError: If view_facet_metadata is True and facet_id_to_view is not given.

    TODO: Add asserts or error handling
    TODO: Update args logic for route_ids to handle optional None type
    TODO: Update logic to handle when view_data and facet_ids override one another
    """
    eia_base_url = EIA_BASE_URL
    api_key_substr = "?api_key=" + api_key

    route_id_substr = ""
    if route_ids:
        route_id_substr = "/".join(route_ids)

    view_data_substr = ""
    if view_data_size:
        view_data_substr = "/data"

    view_facet_metadata_substr = ""
    if view_facet_metadata:
        if not facet_id_to_view:
            raise ValueError("facet_id_to_view must be nonempty when view_facet_metadata is True.")
        view_facet_metadata_substr = f"/facet/{facet_id_to_view}"

    return f"{eia_base_url}{route_id_substr}{view_data_substr}{view_facet_metadata_substr}{api_key_substr}"

def _build_get_url(
    api_key: str,
    data_params: dict,
    batch_params: Optional[dict] = None,
    # route_ids: List[str], # not optional!
    # data_columns: List[str],
    # facet_ids_to_filter: Optional[dict] = None, # dict of lists or make sep arg? id-value pairs
    # freq_filter: Optional[str] = None, # str of freq type
    # start_date_filter: Optional[str] = None,
    # end_date_filter: Optional[str] = None,
    # sort_by: Optional[dict] = None, # dict? col name and direction pairs (asc/desc)
    # subset_n_rows: Optional[int] = None,
    # offset_n_rows: Optional[int] = None,
    verbose: bool = False
) -> str:
    """
    TODO: Convert to accept only 4 things: apikey, data params, batch params, verbose
    TODO: if batch params None, make them empty/defaults
    """
    eia_base_url = EIA_BASE_URL

    # Add explanation header for if verbose and better formatting
    # if verbose: print("Verbosity message here.")

    route_id_substr = "/".join(data_params["route_ids"])
    if verbose: print(f"Route ID substring: {route_id_substr}")

    api_key_substr = f"?api_key={api_key}"

    data_cols_substr = ""
    for col_name in data_params["data_columns"]:
        data_cols_substr += f"&data[]={col_name}"
    if verbose: print(f"Data columns substring: {data_cols_substr}")

    url = f"{eia_base_url}{route_id_substr}/data{api_key_substr}{data_cols_substr}"

    # Append optional data filters

    # Facets
    facet_filters_substr = ""
    if data_params["facet_ids"]:
        for id, value in data_params["facet_ids"].items():
            facet_filters_substr += f"&facets[{id}][]={value}"
    if verbose: print(f"Facet filters subtring: {facet_filters_substr}")

    # Frequency
    freq_substr = ""
    if data_params["freq"]:
        freq_substr = f"&frequency={data_params["freq"]}"
    if verbose: print(f"Frequency substring: {freq_substr}")
    
    # Dates
    start_date_substr = ""
    if data_params["start_date"]:
        start_date_substr = f"&start={data_params["start_date"]}"

    end_date_substr = ""
    if data_params["end_date"]:
        end_date_substr = f"&end={data_params["end_date"]}"

    date_range_substr = f"{start_date_substr}{end_date_substr}"
    if verbose: print(f"Date Range substring: {date_range_substr}")

    # Sort
    sort_substr = ""
    if data_params["sort_by"]:
        for pref, (col_name, dir) in enumerate(data_params["sort_by"].items()):
            sort_col_name_substr = f"&sort[{pref}][column]={col_name}"
            sort_dir_substr = f"&sort[{pref}][direction]={dir}"
            sort_substr += f"{sort_col_name_substr}{sort_dir_substr}"
    if verbose: print(f"Sort substring: {sort_substr}")

    # Optional batch params
    subset_substr = ""
    offset_substr = ""
    
    if batch_params:
        if batch_params["subset"]:
            subset_substr = f"&length={batch_params["subset"]}"
            if verbose: print(f"Subset substring: {subset_substr}")

        if batch_params["offset"]:
            offset_substr = f"&offset={batch_params["offset"]}"
            if verbose: print(f"Offset substring: {offset_substr}")

    # Build get data URL
    url += f"{facet_filters_substr}{freq_substr}{date_range_substr}"
    url += f"{sort_substr}{subset_substr}{offset_substr}"

    return url

def _make_url_request(url: str):
    """
    Make a request to retrieve a URL response from the given url.

    Args:
        url: String of URL to make the request to.

    Returns:
        The raw response object, if the given URL returns a status code of 200.

    Raises:
        ValueError: If the given URL returns a status code other than 200 (invalid request).

    TODO: Determine if error handling is in right place: wrap in try-else?
    TODO: Update with correct exception type
    TODO: Out of scope, but consider making this function a common.py one since it is agnostic to any pipeline.
    """
    response = requests.get(url)
    status_code = response.status_code

    if status_code == 200:
        return response
    else:
        url_without_api_key = url.split("api_key=")[0] + "api_key=..."
        raise ValueError(f"Failed request with status code {status_code} for given url: {url_without_api_key}")

def _check_valid_api_key(api_key: str):
    """
    Check if the given EIA API key is valid.

    Args:
        api_key: The user's EIA API key as a string.

    Returns:
        Boolean representing if the API key is valid or not.

    Raises:
        ValueError: If the response object's status code is not 200 (invalid request).

    TODO: Determine what to display for success/fail: prints, raise exception, return False, etc.
    TODO: Add specific exception handling
    TODO: Determine if a different request method is better instead of retrieving entire response, e.g., not requests.get
    """

    url_to_check = _build_view_url(api_key=api_key)

    try:
        response = _make_url_request(url=url_to_check)
        return response.status_code == 200
    except ValueError as e:
        raise ValueError(f"Something went wrong! Check your EIA API key.\nReceived error: {e}")

def _get_valid_route_ids(api_key: str, route_ids: List[str]) -> tuple:
    """
    Make iterative client requests to check if each of the route IDs are valid and terminal.

    A valid route ID is one that exists in the response JSON object for that route. A terminal route ID
    does not contain any further subroutes (i.e., and there exists a dataset for that route ID lineage).

    The validation check halts at the first instance of an invalid route ID which will result in unchecked
    route IDs having NoneType values in the return lists.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage.
            An empty or None route_ids list will return the base EIA API URL.

    Returns:
        route_ids_valid: List of Booleans or NoneTypes representing if the corresponding route ID is valid or not.
            None means it was not checked due to a higher failure.
        route_ids_terminal: List of Booleans or NoneTypes representing if the corresponding route ID is
            terminal or not. None means it was not checked due to a failure (invalid route ID).
        failing_route_id: Invalid route ID that resulted in the first failure as a string, if available.

    TODO: Build better exception handling for make request
    TODO: Update typehinting: better object stating
    """
    if route_ids == []:
        return [], [], None

    route_ids_valid = [None]*len(route_ids)
    route_ids_terminal = [None]*len(route_ids)
    failing_route_id = None

    for n_routes in range(len(route_ids)):
        route_ids_sublist = route_ids[:n_routes + 1]
        route_url = _build_view_url(api_key=api_key, route_ids=route_ids_sublist)

        try:
            response = _make_url_request(url=route_url)
            response_dict = response.json()["response"]
        except Exception as e: # will need to except only the case where the parent route id is wrong, will need to let it raise any other make request error
            route_ids_valid[n_routes] = False
            failing_route_id = route_ids_sublist[-1]
            # print(f"Exception raised was:\n{e}") # check # Incorrect route ids give a 404
            break

        if response_dict["id"] == route_ids_sublist[-1]:
            route_ids_valid[n_routes] = True
            is_terminal = "routes" not in response_dict.keys()
            route_ids_terminal[n_routes] = is_terminal
        else:
            route_ids_valid[n_routes] = False
            failing_route_id = route_ids_sublist[-1]
            break

    return route_ids_valid, route_ids_terminal, failing_route_id

def _get_subroutes(api_key: str, route_ids: Optional[List[str]] = None) -> dict:
    """
    Display the non-terminal route information for the given URL's route lineage as a pandas dataframe.

    If the route lineage is terminal or there is an error in the URL request, a ValueError will be raised.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage.
            An empty or None route_ids list will display the top-level route IDs. Defaults to None.

    Raises:
        ValueError: If the "routes" key is not found in the returned response object.
    """
    url = _build_view_url(api_key=api_key, route_ids=route_ids)
    response = _make_url_request(url=url)
    response_dict = response.json()["response"]

    if "routes" not in response_dict.keys():
        raise ValueError('"routes" key not found in returned response object.')

    if route_ids == []:
        print("No valid route IDs were found.")
        print(f"Here are the valid route IDs to choose from.\n")
    else:
        route_id_path = "/".join(route_ids)
        print(f"No terminal route IDs found.")
        print(f"Here are the valid route IDs to choose from for route path: {route_id_path}\n")

    routes_df = pd.DataFrame(response_dict["routes"])
    routes_df.columns = ["route_" + col for col in routes_df.columns]

    return {"subroutes": routes_df}

def _build_route_lineage(raw_route_ids: List[str]) -> str:
    """
    Create a string representation of the route IDs as a simple DAG.

    Args:
        raw_route_ids: List of strings of route IDs in order from highest to lowest in their lineage.

    Returns:
        The formatted DAG of the route ID lineage as a string.
    """
    cleaned_routes_ids = []
    for route in raw_route_ids:
        cap_route_list = [word.capitalize() for word in route.split("-")]
        cleaned_routes_ids.append(" ".join(cap_route_list))

    return " -> ".join(cleaned_routes_ids)

def _extract_facet_metadata(
    api_key: str,
    route_ids: List[str],
    base_metadata_response: dict,
    facet_ids: Optional[List[str]]
) -> dict:
    """
    Extract and format facet and subfacet metadata for a terminal route.

    Validates the given facet IDs against the facets listed in the base metadata response. If no
    facet IDs are given, or none of the given ones are valid, subfacet metadata is retrieved for
    all available facets instead.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage,
            for a terminal route.
        base_metadata_response: The base metadata response dict for the route (the "response" key's
            value), used to validate facet_ids against the route's available facets.
        facet_ids: List of facet ID strings to retrieve subfacet metadata for. An empty list or None
            will retrieve subfacet metadata for all available facets.

    Returns:
        Dict with keys "counts" (a dataframe of subfacet counts per valid facet ID), "subfacets"
        (a dict mapping each valid facet ID to its subfacet metadata dataframe), and "empty_facets"
        (a Boolean, True if metadata was retrieved for all facets because no valid facet IDs were
        requested).

    TODO: Figure out how to not have it run the pd.df extractions if facet_ids is []
    """
    bmr_facets = [facet["id"] for facet in base_metadata_response["facets"]]
    empty_valid_facet_ids = False

    # If empty facet_ids, retrieve all info for all facets.
    if facet_ids == []:
        empty_valid_facet_ids = True
        valid_facet_ids = bmr_facets
        print("No facet IDs were passed in.")
    else:
        # Validate facet IDs
        facet_ids_valid = [facet_id in bmr_facets for facet_id in facet_ids]

        if not all(facet_ids_valid):
            valid_facet_ids = [facet for facet, valid in zip(facet_ids, facet_ids_valid) if valid]
            invalid_facet_ids = [facet for facet, valid in zip(facet_ids, facet_ids_valid) if not valid]

            if valid_facet_ids == []:
                empty_valid_facet_ids = True
                valid_facet_ids = bmr_facets
                print(f"No valid facet IDs were found in the given list.\n")

            else:
                print(f"Facet IDs {invalid_facet_ids} were invalid.")
                print("Results will only display for remaining valid facet IDs.\n")

        else:
            valid_facet_ids = facet_ids

    # Extract metadata per facet ID
    subfacet_counts = []
    facet_metadata_dfs = []

    for facet_id in valid_facet_ids:
        facet_metadata_url = _build_view_url(
            api_key=api_key,
            route_ids=route_ids,
            view_facet_metadata=True,
            facet_id_to_view=facet_id
        )
        facet_metadata_response = _make_url_request(url=facet_metadata_url)
        fmr = facet_metadata_response.json()["response"]

        subfacet_counts.append(fmr["totalFacets"])

        # Add logic gate here for if empty_facets is True and make facet_metadata_df a None
        subfacet_df = pd.DataFrame(fmr["facets"])
        subfacet_df.columns = ["subfacet_" + col for col in subfacet_df.columns]
        facet_metadata_dfs.append(subfacet_df)

    subfacet_counts_df = pd.DataFrame(
        zip(valid_facet_ids, subfacet_counts),
        columns=["Facet ID", "Subfacet Count"]
    )

    subfacet_metadata = dict(zip(valid_facet_ids, facet_metadata_dfs))

    return {"counts": subfacet_counts_df, "subfacets": subfacet_metadata, "empty_facets": empty_valid_facet_ids}

def _extract_metadata(
    api_key: str,
    route_ids: List[str],
    get_subfacet_metadata: bool = False,
    facet_ids_to_view: List[str] = None
) -> dict:
    """
    Retrieve and format base metadata for a terminal route, optionally with subfacet metadata.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage,
            for a terminal route.
        get_subfacet_metadata: Whether to also retrieve subfacet metadata for the route's facets.
            Defaults to False.
        facet_ids_to_view: List of facet ID strings to retrieve subfacet metadata for, used only
            when get_subfacet_metadata is True. An empty list or None will retrieve metadata for
            all available facets. Defaults to None.

    Returns:
        Dict with keys "base" (a dict of base metadata dataframes: "general", "date_ranges",
        "defaults", "frequency", "facets", and "data") and "facets" (the dict returned by
        _extract_facet_metadata when get_subfacet_metadata is True, else None).
    """
    base_metadata_url = _build_view_url(api_key=api_key, route_ids=route_ids)
    base_metadata_response = _make_url_request(url=base_metadata_url)
    bmr = base_metadata_response.json()["response"]

    size_metadata_url = _build_view_url(api_key=api_key, route_ids=route_ids, view_data_size=True)
    size_metadata_response = _make_url_request(url=size_metadata_url)
    smr = size_metadata_response.json()["response"]

    n_rows = smr["total"]

    route_desc = bmr["description"].replace("\r\n", " ")
    route_desc = " ".join(route_desc.split())

    route_lineage = _build_route_lineage(raw_route_ids=route_ids)
    full_route_id = "/".join(route_ids)

    general_df = pd.DataFrame(
        [bmr["id"], full_route_id, route_lineage, n_rows, bmr["name"], route_desc],
        index=["Route ID", "Full Route ID", "Route Lineage",  "Row Count", "Route Name", "Description"],
        columns=[""]
    )

    date_ranges_df = pd.DataFrame(
        [bmr["startPeriod"], bmr["endPeriod"]],
        index=["Start Period", "End Period"],
        columns=["period"]
    ).reset_index(names=["date"])

    defaults_df = pd.DataFrame(
        [bmr["defaultDateFormat"], bmr["defaultFrequency"]],
        index=["Default Date Format", "Default Frequency"],
        columns=["value"]
    ).reset_index(names=["setting"])

    freq_df = pd.DataFrame(bmr["frequency"])
    freq_df.columns = ["frequency_" + col for col in freq_df.columns]

    facets_df = pd.DataFrame(bmr["facets"])
    facets_df.columns = ["facet_" + col for col in facets_df.columns]

    data_df = pd.DataFrame(bmr["data"]).transpose().reset_index(names=["id"])
    data_df.columns = ["data_" + col for col in data_df.columns]

    base_metadata_dfs = {
        "general": general_df,
        "date_ranges": date_ranges_df,
        "defaults": defaults_df,
        "frequency": freq_df,
        "facets": facets_df,
        "data": data_df
    }

    # Optional subfacet information
    facets_metadata = None
    if get_subfacet_metadata:
        if not facet_ids_to_view:
            facet_ids_to_view = []

        facets_metadata = _extract_facet_metadata(
            api_key=api_key,
            route_ids=route_ids,
            base_metadata_response=bmr,
            facet_ids=facet_ids_to_view
        )

    return {"base": base_metadata_dfs, "facets": facets_metadata, "response": bmr}

def _side_by_side(*dfs, titles: List[str], align: str = "right"):
    """
    Format and display a given list of dataframes to be side by side using HTML with title headers.

    Args:
        *dfs: List of pandas dataframes
        titles: List of strings for the given dataframes.

    Raises:
        ValueError: If the length of dfs and titles mismatch.
    """
    length_mismatch_assert_msg = (
        f"dfs and titles must be of the same length. "
        f"Received lengths of {len(dfs)} and {len(titles)}."
    )
    assert len(dfs) == len(titles), length_mismatch_assert_msg

    align_values = ["left", "right", "center", "justify"]
    assert align in align_values, f"align must be one of {align_values}, received: {align}"

    html = '<div style="display:flex">'

    for df, title in zip(dfs, titles):
        html += '<div style="margin-right: 2em; text-align:center">'
        html += f'<h4>{title}</h4>'
        df_html = df.to_html()

        if align != "right":
            df_html = df_html.replace("<td>", f'<td style="text-align:{align};">')

        html += df_html
        html += '</div>'

    html += '</div>'

    display(HTML(html))

def _display_metadata(metadata: dict) -> None:
    """
    Display formatted base and facet metadata for a route, side by side, in sections.

    Args:
        metadata: Dict as returned by _extract_metadata, with keys "base" (a dict of metadata
            dataframes) and "facets" (the dict returned by _extract_facet_metadata, or None if
            subfacet metadata was not requested).

    TODO: Explore best align values for each?
    """
    base_metadata = metadata["base"]
    facet_metadata = metadata["facets"]

    spacer = "-"*25
    print(f"{spacer} Route Metadata Info {spacer}")

    _side_by_side(base_metadata["general"], titles=["General Info"], align="justify")
    _side_by_side(base_metadata["date_ranges"], base_metadata["defaults"], titles=["Date Ranges", "Defaults"])
    _side_by_side(base_metadata["frequency"], titles=["Frequencies"])
    _side_by_side(base_metadata["data"], base_metadata["facets"], titles=["Data", "Facets"])

    if facet_metadata is None:
        print("No facet metadata found in response.")
    else:
        subfacet_counts_df = facet_metadata["counts"]
        subfacet_metadata = facet_metadata["subfacets"]
        empty_facets = facet_metadata["empty_facets"]

        if empty_facets:
            print("Displaying subfacet metadata for all available facet IDs.")

        _side_by_side(subfacet_counts_df, titles=["Subfacet Counts by Facet ID"])

        if not empty_facets:
            print(f"Displaying sample subfacet info for requested facet IDs.")
            for facet_id in subfacet_metadata.keys():
                _side_by_side(subfacet_metadata[facet_id].head(5), titles=[f"{facet_id} (sample)"])

def _extract_response(
    api_key: str,
    user_route_ids: list[str],
    route_ids_valid: list[bool],
    route_ids_terminal: list[bool],
    failing_route_id: str | None,
    get_subfacet_metadata: bool = False,
    facet_ids_to_view: Optional[List[str]] = None
) -> dict:
    """
    Display information about the given route lineage from the EIA API.

    If a given route lineage is terminal (i.e., there isn't a further subset of that data), then
    all metadata for that dataset will be displayed. Else, non-terminal routes will display a dataframe
    of available subroutes to select from to find a terminal route ID.

    If a route ID is invalid, the checker will halt and the user will be notified. A valid route will be attempted
    with a higher-level route lineage, if available.

    Args:
        api_key: String of user's EIA API key.
        user_route_ids: List of route IDs the user submitted as strings.
        route_ids_valid: List of Booleans or NoneTypes representing if the corresponding route ID is valid or not.
            None means it was not checked due to a higher failure.
        route_ids_terminal: List of Booleans or NoneTypes representing if the corresponding route ID is
            terminal or not. None means it was not checked due to a failure to be a valid route ID.
        failing_route_id: Invalid route ID that resulting in the first failure as a string, if available.

    Raises:
        AssertionError: If any of the following lists have a mismatch in length: user_route_ids, route_ids_valid, route_ids_terminal
        ValueError: If more than one terminal ID is found (rare).
        ValueError: If an edge case is found in the possible combinations of Boolean logic in the args.

    TODO: Decide if using np.all() is better in if statements
    """
    incorrect_arg_length_assert_msg = f"All route arguments must be of the same length."
    assert len(user_route_ids) == len(route_ids_valid), incorrect_arg_length_assert_msg
    assert len(user_route_ids) == len(route_ids_terminal), incorrect_arg_length_assert_msg
    assert len(route_ids_valid) == len(route_ids_terminal), incorrect_arg_length_assert_msg

    if False in route_ids_valid:
        print(f"Route ID {failing_route_id} is not a valid route ID.\n")
        valid_route_ids_sublist = [route for route, valid in zip(user_route_ids, route_ids_valid) if valid]

        if True not in route_ids_terminal: # all non-terminal -> get subroute display
            return _get_subroutes(api_key=api_key, route_ids=valid_route_ids_sublist)

        elif True in route_ids_terminal: # There is a terminal route -> return metadata display for terminal route
            terminal_route_id = [route for route, terminal in zip(user_route_ids, route_ids_terminal) if terminal][0]
            print(f"Route ID {terminal_route_id} is terminal. Displaying metadata for this dataset.\n")

            return _extract_metadata(
                api_key=api_key,
                route_ids=valid_route_ids_sublist,
                get_subfacet_metadata=get_subfacet_metadata,
                facet_ids_to_view=facet_ids_to_view
            )

        else:
            raise ValueError(f"Some weird edge case was found. Check your route IDs are valid.")

    else: # All route IDs are valid or empty
        if True not in route_ids_terminal: # No terminal routes -> display available options
            return _get_subroutes(api_key=api_key, route_ids=user_route_ids)

        elif True in route_ids_terminal: # Terminal route found, display metadata
            terminal_route_id = user_route_ids[-1] 
            print(f"Route ID {terminal_route_id} is terminal. Displaying metadata for this dataset.\n")

            return _extract_metadata(
                api_key=api_key,
                route_ids=user_route_ids,
                get_subfacet_metadata=get_subfacet_metadata,
                facet_ids_to_view=facet_ids_to_view
            )

        else:
            raise ValueError("Some weird edge case was found. Double check your route IDs list.")

def view_eia_data(
    api_key: str,
    route_ids: Optional[List[str]] = None,
    get_subfacet_metadata: bool = False,
    facet_ids_to_view: Optional[List[str]] = None,
    return_response: bool = False
) -> dict | None:
    """
    View route and metadata information from the EIA API for a given route ID lineage.

    If no route IDs are given, information on the top-level (parent) route IDs is displayed. If the
    given route lineage is terminal, metadata for that dataset is displayed; otherwise, the available
    subroutes are displayed so the user can narrow down to a terminal route ID.

    Args:
        api_key: The user's EIA API key as a string.
        route_ids: List of strings of route IDs in order from highest to lowest in their lineage.
            An empty or None route_ids list will display the top-level route IDs to start from.
            Defaults to None.
        get_subfacet_metadata: Whether to also retrieve and display subfacet metadata when a terminal
            route is found. Defaults to False.
        facet_ids_to_view: List of facet ID strings to retrieve subfacet metadata for, used only when
            get_subfacet_metadata is True. Defaults to None.

    Raises:
        ValueError: If the given API key is invalid.

    TODO: Need to determine what to do if user wants to see the full subfacet metadata for any given set of facets (useful for filtering). When to print samples and when to print full set? Sep func for subfacet viewing?
    """
    _check_valid_api_key(api_key=api_key)

    if route_ids is None or route_ids == []:
        print("No route IDs passed in. Returning information on parent route IDs to start from.\n")
        route_ids = []

    route_ids_valid, route_ids_terminal, failing_route_id = _get_valid_route_ids(api_key=api_key, route_ids=route_ids)

    response = _extract_response(
        api_key=api_key,
        user_route_ids=route_ids,
        route_ids_terminal=route_ids_terminal,
        route_ids_valid=route_ids_valid,
        failing_route_id=failing_route_id,
        get_subfacet_metadata=get_subfacet_metadata,
        facet_ids_to_view=facet_ids_to_view
    )

    if "subroutes" in response.keys():
        # Invalid or non-terminal route path
        display(response["subroutes"])

    else:
        _display_metadata(metadata=response)
    
    return response if return_response else None

def _validate_get_data_params(
    params: dict,
    bmr: dict
) -> None:
    """
    
    """
    # Check for valid datatypes

    # route_ids must be nonempty - check in this func?

    # Data columns
    if len(params["data_columns"]) < 1:
        msg = f"There must be at least one data column to select from."
        raise ValueError(f"{msg} Found {len(params["data_columns"])} data columns.")
    
    bmr_cols = bmr["base"]["data"]["data_id"].tolist()
    for col in params["data_columns"]:
        if col not in bmr_cols:
            msg = f"Data columns {col} was not found in the available data columns for this response."
            raise ValueError(msg)

    # Facet IDs
    bmr_facets = bmr["base"]["facets"]["facet_id"].tolist()
    if params["facet_ids"]:
        for facet_id in params["facet_ids"]:
            if facet_id not in bmr_facets:
                msg = f"Facet ID {facet_id} not found in the available facets for this response."
                raise ValueError(msg)
    # Will need to call teh view facet metadata logic to get valid facet value if wanted
        # else, the query will silently fail or return an empty data response
    # facet values must be in facet ID dict

    # Frequency
    bmr_freq_ids = bmr["base"]["frequency"]["frequency_id"].tolist()
    # Write conditions for if this does not exist -> empty/defaults
    if params["freq"]:
        if params["freq"] not in bmr_freq_ids:
            msg = f'Frequency "{params["freq"]}" was not found in the available frequencies for this response.'
            raise ValueError(msg)

    # freq format must match given format - use regex?

    # Date Ranges - these checks needed? Maybe check the formatting over the range
    # start/end dates mus tbe in range: out of bounds may still be valid just alert user

    # Sorting
    # Write conditions for if this does not exist -> empty/defaults
    if params["sort_by"]:
        for col, dir in params["sort_by"].items():
            if col not in bmr_cols + bmr_facets:
                msg = f"Column {col} not found in teh available columns for this response."
                raise ValueError(msg)
            
            if dir not in ["asc", "desc"]:
                msg = f"The sorint direction for column {col} must be either 'asc' or 'desc'. Received: {dir}."
                raise ValueError(msg)

    # Subset and Offset
    for num, name in zip([params["subset"], params["offset"]], ["subset", "offset"]):
        # ERROR: I need to allow for None values of these two: returns blank/default
            # If None, return blank/default
            # If not None, make checks for convert to int
            # Write conditions for if this does not exist -> empty/defaults
        if num:
            try: 
                num_int = int(num)
            except ValueError as e:
                msg = f"{name} must be a nonnegative int: {e}"
                raise ValueError(msg)
            
            if num_int < 0:
                msg = f"{name} must be a nonnegative int."
                raise ValueError(msg)
            
        # one cannot be greater than the other (valid but does nothing)
        # if offset > total rows then will return empty result -> User won;t knwo that until a valid data query is made and row count is retrieved. Make alert later?

def get_eia_data(
    api_key: str,
    route_ids: List[str], # not optional!
    data_columns: List[str],
    facet_ids_to_filter: Optional[dict] = None, # id-value pairs
    freq_filter: Optional[str] = None, # str of freq type
    start_date_filter: Optional[str] = None,
    end_date_filter: Optional[str] = None,
    sort_by: Optional[dict] = None, # facet_id : facet value
    subset_n_rows: Optional[int] = None,
    offset_n_rows: Optional[int] = None,
    write_to_filepath: str = None,
    verbose: bool = False
) -> pd.DataFrame | None:
    """
    TODO: Rethink verbose param and if it'd be better to make it a verbosity level instead (All True print every url build)
    TODO: Determine the best return for n_rows < MAX: initial response is okay or call _retrieve?
    TODO: Add simple asserts as needed before validation check: nonempty routes, str for filepath, etc.
    TODO: Test function and determine if a msg is needed for if "subroutes" check
    TODO: Get better exception handling for initial response
    """
    # assert route_ids is nonempty list of str

    # View metadata, if possible
    base_metadata_response = view_eia_data(
        api_key=api_key,
        route_ids=route_ids,
        return_response=True
    )

    # Halt here if the above is invalid or nonterminal
    if "subroutes" in base_metadata_response.keys():
        # Print msg?
        return 

    data_params = {
        "route_ids": route_ids,
        "data_columns": data_columns,
        "facet_ids": facet_ids_to_filter,
        "freq": freq_filter,
        "start_date": start_date_filter,
        "end_date": end_date_filter,
        "sort_by": sort_by,
    }
    batch_params = {
        "subset": subset_n_rows,
        "offset": offset_n_rows
    }

    all_params = data_params | batch_params
    _validate_get_data_params(params=all_params, bmr=base_metadata_response)

    # return base_metadata_response # temp check

    # Extract and store data
    try:
        initial_url = _build_get_url(
            api_key=api_key,
            data_params=data_params
        )
        initial_data_request = _make_url_request(url=initial_url)
        initial_data_response = initial_data_request.json()["response"]
    except Exception as e:
        print("Something went wrong. Double check your parameter values.")
        raise e
    
    # Row count check
    n_rows = int(initial_data_response["total"])

    if n_rows > N_MAX_ROWS_EIA_API:
        data_df = _batch_data_request(
            api_key=api_key,
            data_params=data_params,
            n_rows=n_rows,
            verbose=verbose
        )
    else:
        if n_rows == 0:
            print("\nWARNING: The number of rows returned was 0 for the given data params.")
        
        data_df = pd.DataFrame(initial_data_response["data"])

    if write_to_filepath:
        write_dataframe_to_csv(df=data_df, file_path=write_to_filepath)
    else:
        return data_df

# Is this needed? 
def _retrieve_data(
    api_key: str,
    data_params: dict,
    batch_params: dict, # Needed when not init request
    verbose: bool = False
) -> pd.DataFrame:
    """
    
    """

    data_url = _build_get_url(
        api_key=api_key,
        data_params=data_params,
        batch_params=batch_params
    )
    data_request = _make_url_request(url=data_url)
    data_response = data_request.json()["response"]
        # no checks needed since already validated
        # make url-level checks for response code and size of data

    return pd.DataFrame(data_response["data"])

def _batch_data_request(
    api_key: str,
    data_params: dict,
    n_rows: int,
    verbose: bool = False
) -> pd.DataFrame:
    """

    """
    if verbose: print("Batching data request...\n")

    batched_data_dfs = []

    n_max_rows = N_MAX_ROWS_EIA_API
    n_batches = int(np.ceil(n_rows/n_max_rows))

    for batch_num in range(n_batches):
        if verbose: print(f"Batch {batch_num + 1} of {n_batches}")

        batch_params = {
            "subset": n_max_rows, # at most this
            "offset": n_max_rows * batch_num
        }

        batch_data_df = _retrieve_data(
            api_key=api_key,
            data_params=data_params,
            batch_params=batch_params
        )
        batched_data_dfs.append(batch_data_df)

    return pd.concat(batched_data_dfs, ignore_index=True) 
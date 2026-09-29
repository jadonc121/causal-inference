import pandas as pd
import requests

from IPython.display import HTML
from typing import Optional, List, Tuple

## CONSTANTS

EIA_BASE_URL = "https://api.eia.gov/v2/"


## FUNCTIONS


def _make_url_request(url: str):
    """
    TODO: Add docstring
    TODO: Determine if error handling is in right place: wrap in try-else?
    TODO: Create error handling that doesn't print the failing URL with the api key, security oversight
    """
    response = requests.get(url) 
    status_code = response.status_code

    if status_code == 200:
        return response
    else:
        raise ValueError(f"Failed request with status code {status_code} for url: {url}") # security risk here: prints api key

def _check_valid_api_key(api_key: str):
    """
    TODO: Add docstring
    TODO: Determine what to display for success/fail: prints, raise exception, return False, etc.
    TODO: Add specific exception handling
    TODO: Determine if a different request method is better instead of retrieving entire response, e.g., not requests.get
    """

    url_to_check = _build_url_from_route_ids(api_key=api_key)

    try:
        response = _make_url_request(url=url_to_check)
        return response.status_code == 200
    except ValueError as e:
        raise ValueError(f"Something went wrong! Check your EIA API key.\nReceived error: {e}")
    
def _build_url_from_route_ids(api_key: str, route_ids: Optional[List[str]] = None) -> str:
    """
    TODO: Add docstring
    TODO: Add asserts or error handling
    TODO: Update args logic for route_ids to handle optional None type
    """
    eia_base_url = EIA_BASE_URL
    url_api_substring = "api_key=" + api_key

    route_id_substring = ""
    if route_ids:
        route_id_substring = "/".join(route_ids)

    return f"{eia_base_url}{route_id_substring}?{url_api_substring}"  

def _get_valid_route_ids(api_key: str, route_ids: list[str]) -> tuple:
    """
    Iteratively make requests to check for both valid route IDs and terminal route nodes.
    TODO: Add docstring
    TODO: Build better exception handling for make request
    TODO: Update typehinting: better object stating
    """
    # eia_base_url = EIA_BASE_URL
    # url_api_substring = "api_key=" + api_key
    
    route_ids_valid = [None]*len(route_ids)
    route_ids_terminal = [None]*len(route_ids)
    failing_route_id = None

    for n_routes in range(len(route_ids)):
        route_ids_sublist = route_ids[:n_routes + 1]
        # route_id_substring = "/".join(route_ids_sublist)
        # route_url = f"{eia_base_url}{route_id_substring}?{url_api_substring}"
        route_url = _build_url_from_route_ids(api_key=api_key, route_ids=route_ids_sublist)

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

def _side_by_side(*dfs, titles: List[str]):
    """
    TODO: Add docstring
    """
    length_mismatch_assert_msg = (
        f"dfs and titles must be of the same length. "
        f"Received lengths of {len(dfs)} and {len(titles)}."
    )
    assert len(dfs) == len(titles), length_mismatch_assert_msg

    html = '<div style="display:flex">'

    for df, title in zip(dfs, titles):
        html += '<div style="margin-right: 2em; text-align:center">'
        html += f'<h4>{title}</h4>'
        html += df.to_html()
        html += '</div>'

    html += '</div>'

    display(HTML(html))

def _build_route_lineage(raw_route_ids: List[str]) -> str:
    """
    TODO: Add docstring
    """
    cleaned_routes_ids = []
    for route in raw_route_ids:
        cap_route_list = [word.capitalize() for word in route.split("-")]
        cleaned_routes_ids.append(" ".join(cap_route_list))

    return " -> ".join(cleaned_routes_ids)

def _display_metadata(route_ids: list[str], url: str):
    """
    TODO: Add docstring
    TODO: Decide if "routes" check is needed
    TODO: Update args to only need to pass in a single arg: no need for url?
    """
    response = _make_url_request(url=url)
    response_dict = response.json()["response"]

    if "routes" in response_dict.keys():
        raise ValueError(f"Given URL is not terminal.")
    
    # Extract values
    r_id = response_dict["id"]
    r_name = response_dict["name"]
    r_desc = response_dict["description"]

    r_start_period = response_dict["startPeriod"]
    r_end_period = response_dict["endPeriod"]
    r_default_date_format = response_dict["defaultDateFormat"]
    r_default_freq = response_dict["defaultFrequency"]

    r_freq = response_dict["frequency"] # dict
    r_facets = response_dict["facets"] # dict
    r_data = response_dict["data"] # dict

    # Create dfs
    date_ranges_df = pd.DataFrame(
        [r_start_period, r_end_period], 
        index=["Start Period", "End Period"], 
        columns=["period"]
    ).reset_index(names=["date"])

    defaults_df = pd.DataFrame(
        [r_default_date_format, r_default_freq], 
        index=["Default Date Format", "Default Frequency"], 
        columns=["value"]
    ).reset_index(names=["setting"])

    freq_df = pd.DataFrame(r_freq)
    freq_df.columns = ["frequency_" + col for col in freq_df.columns]

    facets_df = pd.DataFrame(r_facets)
    facets_df.columns = ["facet_" + col for col in facets_df.columns]

    data_df = pd.DataFrame(r_data).transpose().reset_index(names=["id"])
    data_df.columns = ["data_" + col for col in data_df.columns]

    # Display values
    spacer = "-"*25
    print(f"{spacer} Route Metadata Info {spacer}\n")

    print(f"Route ID: {r_id}")
    route_lineage = _build_route_lineage(raw_route_ids=route_ids)
    print(f"Route Lineage: {route_lineage}") 
    print(f"Full Route ID: {'/'.join(route_ids)}\n")

    print(f"Route Name: {r_name}")
    print(f"Description: {r_desc}\n")

    _side_by_side(date_ranges_df, defaults_df, titles=["Date Ranges", "Defaults"])
    # _side_by_side(freq_df, facets_df, titles=["Frequencies", "Facets"])
    # _side_by_side(data_df, titles=["Data"])
    _side_by_side(freq_df, titles=["Frequencies"])
    _side_by_side(data_df, facets_df, titles=["Data", "Facets"])

def _display_subroutes_df(url: str) -> pd.DataFrame:
    response = _make_url_request(url=url)
    response_dict = response.json()["response"]

    if "routes" in response_dict.keys():
        routes_df = pd.DataFrame(response_dict["routes"])
        routes_df.columns = ["route_" + col for col in routes_df.columns]
        display(routes_df)
    else:
        raise ValueError('"routes" key not found in returned response object.')

# Build alert user function for valid route ids list
def _display_response_info(
    api_key: str,
    user_route_ids: list[str],
    route_ids_valid: list[bool],
    route_ids_terminal: list[bool],
    failing_route_id: str
) -> str:
    """
    TODO: Add docstring
    TODO: Decide if using np.all() is better in if statements
    TODO: Update func to return a df? How to return a single object from print_metadat? worth the relay?
    TODO: Determine if there's a way to not have to pass in api key as an arg
    TODO: Add a message to print valid route path for invalid or nonterminal paths given (maybe not in this func)
    TODO: Double check logic in all valid and T in terminal list: is double check in if needed?
    """
    incorrect_arg_length_assert_msg = f"All route arguments must be of the same length."
    assert len(user_route_ids) == len(route_ids_valid), incorrect_arg_length_assert_msg
    assert len(user_route_ids) == len(route_ids_terminal), incorrect_arg_length_assert_msg
    assert len(route_ids_valid) == len(route_ids_terminal), incorrect_arg_length_assert_msg
    
    # Check if route_ids is empty - do this outside of this function? Yes b/c I wouldn't call the check valid function if empty

    valid_route_ids_sublist = [route for route, valid in zip(user_route_ids, route_ids_valid) if valid]
    valid_url = _build_url_from_route_ids(api_key=api_key, route_ids=valid_route_ids_sublist)
    valid_route_ids_path = "/".join(valid_route_ids_sublist)

    if False in route_ids_valid:
        print(f"Route ID {failing_route_id} is not a valid route ID.")

        if True not in route_ids_terminal: # all non-terminal -> get subroute display
            print(f"No terminal route IDs found.")
            print(f"Here are the valid route IDs to choose from for route path: {valid_route_ids_path}\n")

            _display_subroutes_df(url=valid_url)

        elif True in route_ids_terminal: # There is a terminal route -> return metadata display for terminal route
            terminal_route_id = [route for route, terminal in zip(user_route_ids, route_ids_terminal) if terminal]
            if len(terminal_route_id) > 1: 
                raise ValueError("More than one terminal ID found. Check your route IDs.")
            
            print(f"Route ID {terminal_route_id[0]} is terminal. Displaying metadata for this dataset.\n")

            _display_metadata(route_ids=valid_route_ids_sublist, url=valid_url)

        else:
            raise ValueError(f"Some weird edge case was found. Check your route IDs are valid.")
        
    else: 
        if True not in route_ids_terminal: # No terminal routes -> display available options
            print(f"No terminal route IDs found.")
            print(f"Here are the valid route IDs to choose from for route path: {valid_route_ids_path}\n")

            _display_subroutes_df(url=valid_url)

        elif True in route_ids_terminal and route_ids_terminal[-1]: # Terminal route found, display metadata
            terminal_route_id = user_route_ids[-1]
            print(f"Route ID {terminal_route_id} is terminal. Displaying metadata for this dataset.\n")

            _display_metadata(route_ids=user_route_ids, url=valid_url)

        else:
            raise ValueError("Some weird edge case was found. Double check your route IDs list.")

## PLACEHOLDERS
def view_eia_data(api_key: str, route_ids: Optional[List[str]] = None):
    """
    TODO: Important logic gate: Check if route_ids is None or empty. If so, do not call latter logic in this func and display parent route nodes.
    """
    # _check_valid_api_key(api_key)

    # _get_valid_route_ids(route_ids)

    # make print or raise problems to user: invalid ids, non-terminal node, terminal node, etc.

    # Call funcs to return a df to display

    # display a df

# get_eia_data(api_key: str, route_ids, facets, etc.) 
    # could call some of the internal funcs of view functions to check for valid subroutes exist, etc.

    # check valid api key

    # check if route list is valid: they exist

    # check for valid facets and other params like date ranges

    # check for data size issues

    # enable batching, if needed

    # formatting

    # returns a df of data? what if it's huge?


# mabye a third func for my pipeline that calls the get function above and writes it automatically?
# run_eia_pipeline(api_key: str, routes, etc)
    # get eia data

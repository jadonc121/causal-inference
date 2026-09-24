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
        raise ValueError(f"Failed request with status code: {status_code} for url: {url}") # security risk here: prints api key

def _check_valid_api_key(api_key: str):
    """
    TODO: Add docstring
    TODO: Determine what to display for success/fail: prints, raise exception, etc.
    TODO: Add specific exception handling
    TODO: Determine if a different request method is better instead of retrieving entire response
    """
    base_url = EIA_BASE_URL
    api_string = "api_key="
    eia_api_key_url_substring = api_string + api_key

    url_to_check = base_url + "?" + eia_api_key_url_substring

    try:
        response = _make_url_request(url=url_to_check)
        return response.status_code == 200
    except ValueError as e:
        raise ValueError(f"Something went wrong! Check your EIA API key.\nReceived error: {e}")
    

def _get_valid_route_ids(api_key: str, route_ids: list[str]) -> tuple:
    """
    Iteratively make requests to check for both valid route IDs and terminal route nodes.
    TODO: Add docstring
    TODO: Build better exception handling for make request
    TODO: Update typehinting: better object stating
    """
    eia_base_url = EIA_BASE_URL
    url_api_substring = "api_key=" + api_key
    
    valid_route_ids = [None]*len(route_ids)
    terminal_route_ids = [None]*len(route_ids)
    failing_route_id = None

    for n_routes in range(len(route_ids)):
        route_ids_sublist = route_ids[:n_routes + 1]
        route_id_substring = "/".join(route_ids_sublist)
        route_url = f"{eia_base_url}{route_id_substring}?{url_api_substring}"

        try:
            response = _make_url_request(url=route_url)
            response_dict = response.json()["response"]
        except Exception as e: # will need to except only the case where the parent route id is wrong, will need to let it raise any other make request error
            valid_route_ids[n_routes] = False
            failing_route_id = route_ids_sublist[-1]
            # print(f"Exception raised was:\n{e}") # check # Incorrect route ids give a 404
            break 

        if response_dict["id"] == route_ids_sublist[-1]:
            valid_route_ids[n_routes] = True

            is_terminal = "routes" not in response_dict.keys()
            terminal_route_ids[n_routes] = is_terminal
        else:
            valid_route_ids[n_routes] = False
            failing_route_id = route_ids_sublist[-1]
            break 
        
    return valid_route_ids, terminal_route_ids, failing_route_id 


def _side_by_side(*dfs, titles):
    """
    TODO: Add docstring
    TODO: Add assert to make len(dfs) == len(titles) (or also allow title to be None)
    """
    html = '<div style="display:flex">'

    for df, title in zip(dfs, titles):
        html += '<div style="margin-right: 2em; text-align:center">'
        html += f'<h4>{title}</h4>'
        html += df.to_html()
        html += '</div>'

    html += '</div>'

    display(HTML(html))


## PLACEHOLDERS

def _metadata_printer(): pass


def view_eia_data(api_key: str, route_ids: Optional[List[str]]): pass
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

import json
import requests

BI_ANALYSIS_CSV_EXPORT_URL = "https://api.brandindex.com/v1/analyses/execute.csv"
BI_SECTORS_URL = "https://api.brandindex.com/v1/taxonomies/regions/{}/sectors"
BI_BRANDS_IN_SECTORS_URL = (
    "https://api.brandindex.com/v1/taxonomies/regions/{}/sectors/{}/brands"
)
AUTH_BRANDINDEX_API_URL = "https://api.brandindex.com/v1/auth/login"

headers = {"content-type": "application/json"}


def authenticate(username, password):
    payload = {
        "data": {"email": username, "password": password},
        "meta": {"version": "v1"},
    }
    session = requests.Session()
    response = session.post(
        AUTH_BRANDINDEX_API_URL, data=json.dumps(payload), headers=headers
    )
    return session


# Function to call the API and extract the data
def run_analysis(session, payload):
    # Call the post request to generate the response.
    response = session.post(
        BI_ANALYSIS_CSV_EXPORT_URL, data=json.dumps(payload), headers=headers
    )
    return response


def get_sectors(session, region):
    response = session.get(BI_SECTORS_URL.format(region), headers=headers)
    return response


def get_brands(session, sector, region):
    response = session.get(
        BI_BRANDS_IN_SECTORS_URL.format(region, sector), headers=headers
    )
    return response

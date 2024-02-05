import json
from typing import final
import copy

analysisid = "InvescoBrandIndexAnalysis"

moving_average = 56
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'

brands = [
    {"brand_id": 1002665, "region": "us", "sector_id": 27},  # Invesco
    {"brand_id": 1000630, "region": "us", "sector_id": 27},  # "Blackrock (ishares)"
    {"brand_id": 27024, "region": "us", "sector_id": 27},  # Vanguard
    {"brand_id": 27009, "region": "us", "sector_id": 27},  # Fidelity
    {"brand_id": 27005, "region": "us", "sector_id": 27},  # "Charles Schwab"
    {"brand_id": 27013, "region": "us", "sector_id": 27},  # "JP Morgan Chase"
    {"brand_id": 1005533, "region": "us", "sector_id": 27},  # "American Funds"
    {"brand_id": 1005532, "region": "us", "sector_id": 27},  # "State Street"
    {"brand_id": 27010, "region": "us", "sector_id": 27},  # "Franklin Templeton"
]


filters = [
    {
        "segment": "Financial Advisors",
        "filters": [
            {"expression": "profiles_us__pdl_employ_jobtitle_2020 in [304]"},
        ],
    },
    {
        "segment": "HNWIs",
        "filters": [
            {"expression": "profiles_us__pdlc_age_banded in [1, 2, 3, 4]"},
            {
                "expression": "profiles_us__pdl_total_value_wealth_assets in [6, 7, 8, 9, 10, 11]"
            },
        ],
    },
    {
        "segment": "Megalennials",
        "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [2]"},
            {
                "expression": "profiles_us__pdl_profile_gross_household in [8, 9, 10, 11, 12, 13, 14, 15, 16]"
            },
        ],
    },
    {
        "segment": "NCAA",
        "filters": [
            {
                "expression": "(profiles_us__pdl_NCAA_College_Basketball_watch_type in [1, 2, 3, 4] or profiles_us__pdl_NCAA_College_Football_watch_type in [1, 2, 3, 4] and profiles_us__pdl_profile_gross_household in [8, 9, 10, 11, 12, 13, 14, 15])"
            },
        ],
    },
    {"segment": "Total Population", "filters": []},
]


DMAs = {}

basejson = '{"data": {"id": "##analysisid##","queries":[], "scoring": "total"},"meta":{"version": "v1"}}'
basejson = basejson.replace("##analysisid##", analysisid)

query_json = ""
queries = []

for filter in filters:
    id = filter["segment"]
    _filters = filter["filters"]
    for brand in brands:
        if DMAs:
            for dma in DMAs:
                _filters_dma = copy.deepcopy(_filters)
                query = {}
                _filters_dma.append(
                    {"expression": "top60dmas2_inputzip in [{}]".format(DMAs[dma])}
                )
                brand_id = brand["brand_id"]
                region = brand["region"]
                sector_id = brand["sector_id"]
                query["id"] = id + "|" + dma
                entity = {}
                entity["brand_id"] = brand_id
                entity["region"] = region
                entity["sector_id"] = sector_id
                query["entity"] = entity
                query["filters"] = _filters_dma
                query["moving_average"] = moving_average
                query["period"] = json.loads(date_period)
                query["metrics_score_types"] = json.loads(metrics_score_types)
                queries.append(query)
            # For all national markets
            query = {}
            brand_id = brand["brand_id"]
            region = brand["region"]
            sector_id = brand["sector_id"]
            query["id"] = id + "|National"
            entity = {}
            entity["brand_id"] = brand_id
            entity["region"] = region
            entity["sector_id"] = sector_id
            query["entity"] = entity
            query["filters"] = _filters
            query["moving_average"] = moving_average
            query["period"] = json.loads(date_period)
            query["metrics_score_types"] = json.loads(metrics_score_types)
            queries.append(query)
        else:
            query = {}
            brand_id = brand["brand_id"]
            region = brand["region"]
            sector_id = brand["sector_id"]
            query["id"] = id
            entity = {}
            entity["brand_id"] = brand_id
            entity["region"] = region
            entity["sector_id"] = sector_id
            query["entity"] = entity
            query["filters"] = _filters
            query["moving_average"] = moving_average
            query["period"] = json.loads(date_period)
            query["metrics_score_types"] = json.loads(metrics_score_types)
            queries.append(query)

final_json = json.loads(basejson)
final_json["data"]["queries"] = queries
final_json

# with open("C:\\Users\\moin.u\\Documents\\NABLER\\XMedia\\BrandIndex_Invesco_spy\\Invesco.json", "w") as f:
#     json.dump(final_json, f)

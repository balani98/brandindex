## 3rd brands
# MA = 12 and new filter conditions
import json
from typing import final
import copy

analysisid = "LFGBrandIndexAnalysis"

moving_average = 84
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'

brands = [
    {"brand_id": 27021, "region": "us", "sector_id": 27},
    {"brand_id": 27024, "region": "us", "sector_id": 27},
    {"brand_id": 27009, "region": "us", "sector_id": 27},
    {"brand_id": 1000341, "region": "us", "sector_id": 27},
    {"brand_id": 1001310, "region": "us", "sector_id": 27},
    {"brand_id": 1001752, "region": "us", "sector_id": 27},
    {"brand_id": 1002056, "region": "us", "sector_id": 27},
    {"brand_id": 1003688, "region": "us", "sector_id": 27},
    {"brand_id": 1003755, "region": "us", "sector_id": 27},
    {"brand_id": 1004059, "region": "us", "sector_id": 27},
    {"brand_id": 25018, "region": "us", "sector_id": 25},
    {"brand_id": 25013, "region": "us", "sector_id": 25},
    {"brand_id": 25016, "region": "us", "sector_id": 25},
    {"brand_id": 25019, "region": "us", "sector_id": 25},
    {"brand_id": 25003, "region": "us", "sector_id": 25},
    {"brand_id": 25017, "region": "us", "sector_id": 25},
    {"brand_id": 25021, "region": "us", "sector_id": 25},
    {"brand_id": 25004, "region": "us", "sector_id": 25},
    {"brand_id": 25011, "region": "us", "sector_id": 25},
    {"brand_id": 25007, "region": "us", "sector_id": 25},
    {"brand_id": 27026, "region": "us", "sector_id": 25},
    {"brand_id": 27015, "region": "us", "sector_id": 25},
    {"brand_id": 1001136, "region": "us", "sector_id": 25},
    {"brand_id": 1001172, "region": "us", "sector_id": 25},
    {"brand_id": 1003515, "region": "us", "sector_id": 25},
    {"brand_id": 1003770, "region": "us", "sector_id": 25},
    {"brand_id": 1005729, "region": "us", "sector_id": 25},
]

filters = [
    {
        "segment": "Affluent Seekers 50-69",
        "filters": [
            {
                "expression": "(profiles_us__pdl_totalassets_excl_home in [7, 8, 9, 10, 11] and bixdemo_agegranular in [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52])"
            },
        ],
    },
    {
        "segment": "Affluent Aspirers 30-49",
        "filters": [
            {
                "expression": "(bixdemo_agegranular in [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32] and (profiles_us__pdl_profile_gross_household in [11, 12, 13, 14, 15, 16] or profiles_us__pdl_totalassets_excl_home in [7, 8, 9, 10, 11]))"
            },
        ],
    },
    {
        "segment": "Mass Middle Aspirers 25-44",
        "filters": [
            {
                "expression": "(profiles_us__pdl_totalassets_excl_home in [1, 2, 3, 4, 5, 6] and profiles_us__pdlc_age_banded in [2, 3] and profiles_us__pdl_profile_gross_household in [7, 8, 9, 10, 11, 12, 13, 14, 15, 16])"
            },
        ],
    },
    {
        "segment": "Mass Middle Aspirers 45+",
        "filters": [
            {
                "expression": "(profiles_us__pdl_profile_gross_household in [7, 8, 9, 10, 11, 12, 13, 14, 15, 16] and profiles_us__pdl_totalassets_excl_home in [1, 2, 3, 4, 5, 6] and bixdemo_agegranular in [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53])"
            },
        ],
    },
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
with open(
    "C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\LFG_dp2brands03.json",
    "w",
) as f:
    json.dump(final_json, f)

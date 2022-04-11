import json
from typing import final
import copy

analysisid = "TorridAnalysis"

moving_average = 56
dma_specific_moving_average = 112
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'


brands = [
    {"brand_id": 9022, "region": "us", "sector_id": 9},
    {"brand_id": 9016, "region": "us", "sector_id": 9},
    {"brand_id": 1001102, "region": "us", "sector_id": 41},
    {"brand_id": 10018, "region": "us", "sector_id": 10},
    {"brand_id": 1005299, "region": "us", "sector_id": 9},
    {"brand_id": 1005297, "region": "us", "sector_id": 9},
    {"brand_id": 1001886, "region": "us", "sector_id": 9},
    {"brand_id": 14010, "region": "us", "sector_id": 14},
    {"brand_id": 10016, "region": "us", "sector_id": 10},
    {"brand_id": 9023, "region": "us", "sector_id": 9},
    {"brand_id": 10003, "region": "us", "sector_id": 10},
    {"brand_id": 9014, "region": "us", "sector_id": 9},
    {"brand_id": 14012, "region": "us", "sector_id": 14},
]

filters = [
    {
        "segment": "F 18-24",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_agegranular in [1,2,3,4,5,6,7]"},
        ],
    },
    {
        "segment": "F 18-44",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {
                "expression": "bixdemo_agegranular in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]"
            },
        ],
    },
    {
        "segment": "F 25-44",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {
                "expression": "bixdemo_agegranular in [8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]"
            },
        ],
    },
    {"segment": "NAT REP (No filter)", "filters": []},
]

DMAs = {
    "Los Angeles CA": "2",
    "Chicago IL": "3",
    "Dallas-Ft. Worth TX": "5",
    "Cleveland-Akron (Canton) OH": "19",
    "Indianapolis IN": "26",
    "San Diego CA": "28",
    "Sacramento-Stockton-Modesto CA": "20",
    "New York, NY": "1",
    "Houston TX": "10",
    "Atlanta GA": "9",
    "Columbus OH": "32",
    "Philadelphia PA": "4",
    "Seattle-Tacoma WA": "13",
    "Detroit MI": "11",
    "Phoenix AZ": "12",
    "Boston MA-Manchester NH": "7",
    "Austin TX": "40",
    "Minneapolis-St. Paul MN": "15",
    "San Antonio, TX": "36",
    "Cincinnati, OH": "35",
    "Pittsburgh, PA": "23",
    "Charlotte, NC": "25",
    "Raleigh-Durham, NC": "24",
}

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
                query["moving_average"] = dma_specific_moving_average
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

with open("path_to\\Torrid.json", "w") as f:
    json.dump(final_json, f)

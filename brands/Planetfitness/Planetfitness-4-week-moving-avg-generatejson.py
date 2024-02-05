# Planet fitness json : 4 week moving avg  ( Genz , millenials , Total Population)
import json
from typing import final
import copy

analysisid = "PlanetFitnessBrandIndexAnalysis"

moving_average = 28
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'
# date_period = '{"end_date": {"date": "2021-10-31"},"start_date": {"date": "2021-10-01"}}'

# questions
# Do we require moving average different for different audiences
# what is Nat rap filter ?
# Which is the correct filter for Age 18-25
# which filter should we take Active Gen Z
brands = [
    {"brand_id": 1003382, "region": "us", "sector_id": 35},  # Beach body
    {"brand_id": 1003380, "region": "us", "sector_id": 35},  # Jenny Craig
    {"brand_id": 1003378, "region": "us", "sector_id": 35},  # Noom
    {"brand_id": 1001795, "region": "us", "sector_id": 35},  # Nordic Trac
    {"brand_id": 1003381, "region": "us", "sector_id": 35},  # Nutrisystem
    {"brand_id": 1001796, "region": "us", "sector_id": 35},  # Orange Theory
    {"brand_id": 1001628, "region": "us", "sector_id": 35},  # Peloton
    {"brand_id": 1001186, "region": "us", "sector_id": 35},  # Planet fitness
    {"brand_id": 1003379, "region": "us", "sector_id": 35},  # WW(Weight Watchers)
    {"brand_id": 1001187, "region": "us", "sector_id": 35},  # Crunch Gym
    {"brand_id": 1006463, "region": "us", "sector_id": 35},  # EOS Fitness
    {"brand_id": 1006464, "region": "us", "sector_id": 35},  # Lifetime Fitness
]


filters = [
    {
        "segment": "GenZ",
        "filters": [
            {"expression": "bixdemo_agegranular in [1, 2, 3, 4, 5, 6, 7, 8, 9]"},
        ],
    },
    {
        "segment": "Millennials",
        "filters": [
            {
                "expression": "bixdemo_agegranular in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]"
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
with open(
    "C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_Planetfitness\\Planetfitness_moving_average_4_week.json",
    "w",
) as f:
    json.dump(final_json, f)

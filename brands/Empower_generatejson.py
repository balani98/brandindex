import json
from typing import final
import copy

analysisid = "EmpowerBrandIndexAnalysis"

moving_average = 84
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'


brands = [
    {"brand_id": 27005, "region": "us", "sector_id": 27},   #"Charles Schwab"
    {"brand_id": 27008, "region": "us", "sector_id": 27},   #"Edward Jones"
    {"brand_id": 1003755, "region": "us", "sector_id": 27}, #"Empower Retirement"
    {"brand_id": 27009, "region": "us", "sector_id": 27},   #"Fidelity"	
    {"brand_id": 27017, "region": "us", "sector_id": 27},   #"Merrill"
	{"brand_id": 25017, "region": "us", "sector_id": 25},   #"Nationwide"
	{"brand_id": 25021, "region": "us", "sector_id": 25},   #"Prudential"
	{"brand_id": 27021, "region": "us", "sector_id": 27},   #"T. Rowe Price"
    {"brand_id": 1001310, "region": "us", "sector_id": 27}, #"TIAA"	
    {"brand_id": 27024, "region": "us", "sector_id": 27},   #"Vanguard"	
    {"brand_id": 1002056, "region": "us", "sector_id": 27}, #"Voya Financial"	
    {"brand_id": 1000632, "region": "us", "sector_id": 27}, #"Wells Fargo Advisors"	
	{"brand_id": 25009, "region": "us", "sector_id": 25},   #"Geico"	
	{"brand_id": 27015, "region": "us", "sector_id": 25},   #"Mass Mutual"	
	{"brand_id": 12001, "region": "us", "sector_id": 12},   #"Bank of America"	
	{"brand_id": 12024, "region": "us", "sector_id": 12},   #"Wells Fargo"	
]


filters = [
    {
    "segment": "Finance Industry", 
    "filters": [
            {"expression": "profiles_us__pdl_industrynaics in [9]"},    
    ],
   },
   {
    "segment": "IA $50K+", 
    "filters": [
            {"expression": "profiles_us__pdl_totalassets_excl_home in [5, 6, 7, 8, 9, 10, 11]"}, 
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

with open("Path_to\\Empower.json", "w") as f:
    json.dump(final_json, f)

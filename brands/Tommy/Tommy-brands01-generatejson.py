# 1st brands
import json
from typing import final
import copy

analysisid = "TommyBrandIndexAnalysis"

moving_average = 56
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'
#date_period = '{"end_date": {"date": "2021-10-31"},"start_date": {"date": "2021-10-01"}}'



brands = [
    {"brand_id": 1001884, "region": "us", "sector_id": 10},    
    {"brand_id": 41013, "region": "us", "sector_id": 41},   
    {"brand_id": 41005, "region": "us", "sector_id": 41}, 
    {"brand_id": 1002105, "region": "us", "sector_id": 10},   
    {"brand_id": 42014, "region": "us", "sector_id": 41},  
]



filters = [
    {
    "segment": "National", 
    "filters": [
            {"expression": "bixdemo_gender in (1,2)"},    
    ],
   },
   {
    "segment": "Male", 
    "filters": [
            {"expression": "bixdemo_gender in [1]"}, 
    ],
   }, 
   {
    "segment": "Female", 
    "filters": [
            {"expression": "bixdemo_gender in [2]"},    
    ],
   },
   {
    "segment": "Very Likely", 
    "filters": [
            {"expression": "inmarket_clothing in [1]"}, 
    ],
   },
   {
    "segment": "Likely", 
    "filters": [
            {"expression": "inmarket_clothing in [2]"},    
    ],
   },
   {
    "segment": "Somewhat Likely", 
    "filters": [
            {"expression": "inmarket_clothing in [3]"}, 
    ],
   },
    {
    "segment": "Gen Z (2000 and later)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [1]"},    
    ],
   },
   {
    "segment": "Millennial (1982-1999)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [2]"}, 
    ],
   },
   {
    "segment": "Gen X (1965-1964)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [3]"},    
    ],
   },
   {
    "segment": "Baby Boomer (1946-1964)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [4]"}, 
    ],
   },
   {
    "segment": "Silent Generation (1928-1945)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [5]"},    
    ],
   },
   {
    "segment": "Pre-Silent Generation (1927 and earlier)", 
    "filters": [
            {"expression": "profiles_us__pdlc_omni_generation in [6]"}, 
    ],
   },
   {
    "segment": "in-store", 
    "filters": [
            {"expression": "profiles_us__pdl_howbuy_clothing in [1]"},    
    ],
   },
   {
    "segment": "online", 
    "filters": [
            {"expression": "profiles_us__pdl_howbuy_clothing in [2]"}, 
    ],
   },
   {
    "segment": "both", 
    "filters": [
            {"expression": "profiles_us__pdl_howbuy_clothing in [3]"}, 
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
with open("C:\\Users\\moin.uddin\\Documents\\NABLER\\XMedia\\BrandIndex_EWC\\Tommy_brands01.json", "w") as f:
    json.dump(final_json, f)

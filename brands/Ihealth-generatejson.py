
import json
from typing import final
import copy

analysisid = "IHealthBrandIndexAnalysis"

moving_average = 56
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'
#date_period = '{"end_date": {"date": "2021-10-31"},"start_date": {"date": "2021-10-01"}}'



brands = [
     {"brand_id": 1006144, "region": "us", "sector_id": 21},   #AZO   
     {"brand_id": 1002189, "region": "us", "sector_id": 21},   #Culturelle
     {"brand_id": 1006140, "region": "us", "sector_id": 21},   #Align
     {"brand_id": 1006141, "region": "us", "sector_id": 21},   #Garden of Life
     {"brand_id": 1006142, "region": "us", "sector_id": 21},   #Honeypot
     {"brand_id": 1006143, "region": "us", "sector_id": 21},   #Ph-D
     {"brand_id": 1004426, "region": "us", "sector_id": 21},   #Nature's bounty
     {"brand_id": 1004425, "region": "us", "sector_id": 21},   #OLLY
     {"brand_id": 22012, "region": "us", "sector_id": 21},     #Enfamil
     {"brand_id": 1006145, "region": "us", "sector_id": 21},   #Estroven
]



filters = [
    {"segment": "Total Population",      "filters": []},
    {
    "segment": "A25-49", 
    "filters": [
            {"expression": "bixdemo_agegranular in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]"},    
    ],
   },
    {
    "segment": "A25-49 & Supplement types purchased: probiotics", 
    "filters": [
            {"expression": "bixdemo_agegranular in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32] and profiles_us__pdl_supplements_purchased in ['pdl_supplements_purchased_7']"},    
    ],
   },
   {
    "segment": "Ages of children <18 in HH: 12 or under & Supplement types purchased: probiotics, vitamins, dietary minerals, essential fatty acids, natural supplements, other", 
    "filters": [
            {"expression": "profiles_us__pdlc_child_ages_mc_2019 in ['pdlc_child_ages_mc_2019_1', 'pdlc_child_ages_mc_2019_2', 'pdlc_child_ages_mc_2019_3', 'pdlc_child_ages_mc_2019_4', 'pdlc_child_ages_mc_2019_5', 'pdlc_child_ages_mc_2019_6', 'pdlc_child_ages_mc_2019_7', 'pdlc_child_ages_mc_2019_8', 'pdlc_child_ages_mc_2019_9', 'pdlc_child_ages_mc_2019_10', 'pdlc_child_ages_mc_2019_11', 'pdlc_child_ages_mc_2019_12', 'pdlc_child_ages_mc_2019_13'] and profiles_us__pdl_supplements_purchased in ['pdl_supplements_purchased_7', 'pdl_supplements_purchased_1', 'pdl_supplements_purchased_2', 'pdl_supplements_purchased_5', 'pdl_supplements_purchased_6', 'pdl_supplements_purchased_97']"},    
    ],
   },
   {
    "segment": "F25-44", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [2] and bixdemo_agegranular in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27]"}, 
    ],
   },
   {
    "segment": "F18-24", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [2] and bixdemo_agegranular in [1, 2, 3, 4, 5, 6, 7]"},    
    ],
   },
   {
    "segment": "F45-55", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [2] and bixdemo_agegranular in [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38]"}, 
    ],
   },
   {
    "segment": "F45-64", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [2] and bixdemo_agegranular in [28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47]"}, 
    ],
   },
   {
    "segment": "HCPs", 
    "filters": [
            {"expression": "profiles_us__pdl_jobtitle in [22]"}, 
    ],
   },
    {
    "segment": "K&B 0-2", 
    "filters": [
            {"expression": "profiles_us__pdlc_child_ages_mc_2019 in ['pdlc_child_ages_mc_2019_1', 'pdlc_child_ages_mc_2019_2', 'pdlc_child_ages_mc_2019_3'] and profiles_us__pdl_supplements_purchased in ['pdl_supplements_purchased_2', 'pdl_supplements_purchased_5', 'pdl_supplements_purchased_6', 'pdl_supplements_purchased_97', 'pdl_supplements_purchased_7', 'pdl_supplements_purchased_1']"}, 
    ],
   },
   {
    "segment": "K&B 3-12", 
    "filters": [
            {"expression": "profiles_us__pdlc_child_ages_mc_2019 in ['pdlc_child_ages_mc_2019_4', 'pdlc_child_ages_mc_2019_5', 'pdlc_child_ages_mc_2019_6', 'pdlc_child_ages_mc_2019_7', 'pdlc_child_ages_mc_2019_8', 'pdlc_child_ages_mc_2019_9', 'pdlc_child_ages_mc_2019_10', 'pdlc_child_ages_mc_2019_11', 'pdlc_child_ages_mc_2019_12', 'pdlc_child_ages_mc_2019_13'] and profiles_us__pdl_supplements_purchased in ['pdl_supplements_purchased_2', 'pdl_supplements_purchased_5', 'pdl_supplements_purchased_6', 'pdl_supplements_purchased_97', 'pdl_supplements_purchased_7', 'pdl_supplements_purchased_1']"}, 
    ],
   },
   {
    "segment": "F25-49", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [2] and bixdemo_agegranular in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]"}, 
    ],
   },
   {
    "segment": "M25-49", 
    "filters": [
            {"expression": "profiles_us__pdl_gender in [1] and bixdemo_agegranular in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]"}, 
    ],
   },
   {
    "segment": "DMA Gen Pop (for Culturelle Comparison)", 
    "filters": [
            {"expression": "profiles_us__pdlc_inputzipdma_recode in [527, 534, 539, 560, 561, 609, 613, 623, 659, 751, 770, 820]" }, 
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
with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\XMedia\\BranIndex_IHealth\\IHealth.json", "w") as f:
    json.dump(final_json, f)
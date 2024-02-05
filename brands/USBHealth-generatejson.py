import json
from typing import final
import copy

analysisid = "USBBrandHealthBrandIndexAnalysis"

moving_average = 84
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'


brands = [
    {"brand_id": 12006, "region": "us", "sector_id": 12},   #"Citibank"
    {"brand_id": 53496, "region": "us", "sector_id": 12},   #"Capital One"	
    {"brand_id": 12017, "region": "us", "sector_id": 12},   #"PNC" 
	{"brand_id": 12009, "region": "us", "sector_id": 12},   #"Fifth-Third"
	{"brand_id": 1001320, "region": "us", "sector_id": 12},   #"TCF" 
	{"brand_id": 1003055, "region": "us", "sector_id": 12},   #"Truist"
	{"brand_id": 12001, "region": "us", "sector_id": 12},   #"Bank of America"	
	{"brand_id": 12024, "region": "us", "sector_id": 12},   #"Wells Fargo"	
    {"brand_id": 12005, "region": "us", "sector_id": 12},   #"Chase"	
    {"brand_id": 12021, "region": "us", "sector_id": 12},   #"USBank"	
    {"brand_id": 12018, "region": "us", "sector_id": 12},   #"Regions Bank"	
    
]


filters = [
    {
    "segment": "A25-54", 
    "filters": [
            {"expression": "profiles_us__pdlc_age_banded in [2, 3, 4]"},    
    ],
   },
#    {
#     "segment": "Young Affluent-A",
#     "filters": [
#             {"expression": "(bixdemo_agegranular in [4, 5, 6, 7] and profiles_us__pdl_profile_gross_household in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]) or (bixdemo_agegranular in [8, 9, 10, 11, 12] and profiles_us__pdl_profile_gross_household in [8, 9, 10, 11, 12, 13, 14, 15, 16]) or (bixdemo_agegranular in [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23] and profiles_us__pdl_profile_gross_household in [9, 10, 11, 12, 13, 14, 15, 16]) or (bixdemo_agegranular in [24, 25, 26, 27] and profiles_us__pdl_profile_gross_household in [10, 11, 12, 13, 14, 15, 16]) and profiles_us__pdl_educ in [3, 4, 5, 6] and profiles_us__pdl_empl_stat_2018 in [1] and profiles_us__pdl_decisionhhfinancial in [1, 3]"}, 
#     ],
#    },
      {
     "segment": "Young Affluent",
     "filters": [
             {"expression": "bixdemo_ageall in [1, 2] and profiles_us__pdl_totalassets_excl_home  in [6, 7, 8]"}, 
     ],
    },
   {
    "segment": "Mid-Life Affluent",
    "filters": [
            {"expression": "bixdemo_ageall in [3, 4, 5]  and profiles_us__pdl_totalassets_excl_home  in [7, 8]"}, 
    ],
   },
#    {
#     "segment": "Small Business Owners", 
#     "filters": [
#             {"expression": "(profiles_us__pdl_management_level in [1] or profiles_us__pdl_employee_status in [2] or profiles_us__pdl_self_employ_type_2018 in [1]) and profiles_us__pdl_companyrev in [1, 2, 3, 4, 5, 6]"},    
#     ],
#    },
    {
    "segment": "Small Business Owners", 
    "filters": [
            {"expression": "profiles_us__pdl_companyrev in [1, 2, 3, 4, 5, 6] and (profiles_us__pdl_jobtitle in [2, 4] or profiles_us__pdl_employ_jobtitle_2020 in [301, 303, 304, 399] or profiles_us__pdl_self_employ_type_2018 in [1])"},    
    ],
   },
   {
    "segment": "Corporate Segment", 
    "filters": [
            {"expression": "profiles_us__pdl_B2B_decision in ['pdl_B2B_decision_15'] or profiles_us__pdl_employ_decisionmaker_main in ['pdl_employ_decisionmaker_main_15'] or profiles_us__pdl_jobtitle in [4]"},    
    ],
   },
   {
    "segment": "African American", 
    "filters": [
            {"expression": "profiles_us__pdl_race in [2]"},    
    ],
   },
   {
    "segment": "Asian", 
    "filters": [
            {"expression": "profiles_us__pdl_race in [4]"},    
    ],
   },
   {
    "segment": "Hispanic", 
    "filters": [
            {"expression": "profiles_us__pdl_race in [3]"},    
    ],
   },
   {
    "segment": "Wealth Management", 
    "filters": [
            {"expression": "profiles_us__pdl_totalassets_excl_home in [7, 8, 9]"},    
    ],
   },
   {"segment": "Total Population", "filters": []},
   {
        "segment": "U.S. Bank Footprint",
        "filters": [{"expression": "profiles_us__pdl_inputstate in [4, 5, 6, 8, 16, 17, 18, 19, 20, 21, 27, 29, 30, 31, 32, 35, 37, 38, 39, 41, 46, 47, 49, 53, 55, 56]"},],
    },
    {
        "segment": "Seattle",
        "filters": [{"expression": "top60dmas2_inputzip in [13]"},],
    },
    {
        "segment": "Charlotte",
        "filters": [{"expression": "top60dmas2_inputzip in [25]"},],
    },
    {
        "segment": "Portland, OR",
        "filters": [{"expression": "top60dmas2_inputzip in [22]"},],
    },
   {
        "segment": "Sacramento",
        "filters": [{"expression": "top60dmas2_inputzip in [20]"},],
    },
    {
        "segment": "Los Angeles",
        "filters": [{"expression": "top60dmas2_inputzip in [2]"},],
    },
    {
        "segment": "San Francisco",
        "filters": [{"expression": "top60dmas2_inputzip in [6]"},],
    },
    {
        "segment": "San Diego",
        "filters": [{"expression": "top60dmas2_inputzip in [28]"},],
    },
    {
        "segment": "Minneapolis",
        "filters": [{"expression": "top60dmas2_inputzip in [15]"},],
    },
    {
        "segment": "Union Bank",
        "filters": [{"expression": "profiles_us__pdlc_inputzipdma_recode in [803, 807, 819, 820, 825, 862]"},],
    },
    {
        "segment": "California",
        "filters": [{"expression": "profiles_us__pdlc_inputzipdma_recode in [803, 807, 825, 862]"},],
    },
    {
        "segment": "Southern California",
        "filters": [{"expression": "profiles_us__pdlc_inputzipdma_recode in [803, 825]"},],
    },
    {
        "segment": "In market - Deposits",
        "filters": [{"expression": "bank_account_future in [1,2,4]"},],
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

with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_USBHealthAPI\\USBHealth.json", "w") as f:
    json.dump(final_json, f)

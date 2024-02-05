## EWC Final 
import json
from typing import final
import copy

analysisid = "EWC_BrandIndexAnalysis"

moving_average = 84
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'
#date_period = '{"end_date": {"date": "2021-10-31"},"start_date": {"date": "2021-10-01"}}'



brands = [
    {"brand_id": 1001796, "region": "us", "sector_id": 35},   #'Orange Theory'
    {"brand_id": 1003667, "region": "us", "sector_id": 1001}, #'European Wax Center'
    {"brand_id": 1003720, "region": "us", "sector_id": 1001}, #'Benefit Cosmetics & Boutique'
    {"brand_id": 1003721, "region": "us", "sector_id": 1001}, #'Bliss Spa'
    {"brand_id": 1004013, "region": "us", "sector_id": 1001}, #'Pretty Kitty'   
    {"brand_id": 1001163, "region": "us", "sector_id": 16}, # Wingstop   
     {"brand_id": 1001186, "region": "us", "sector_id": 35}, # Planetfitness  
]



filters = [
    {
    "segment": "All Ages", 
    "filters": [
            {"expression":"bixdemo_gender in [1, 2]"},    
    ],
   },
    {
        "segment": "Female + 18-24",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_ageall in [1]"},
        ],
    },
    {
        "segment": "Female + 25-34",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_ageall in [2]"},
        ],
    },
    {
        "segment": "Female + 35-54",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_pepsiage2 in [3]"},
        ],
    },
    {
        "segment": "Female + 55+",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_ageall in [5, 6]"},
        ],
    },
    {
    "segment": "Male", 
    "filters": [
            {"expression":"bixdemo_gender in [1]"},    
    ],
   },
    {
    "segment": "African American", 
    "filters": [
            {"expression":"bixdemo_ethnicity in [3]"},    
    ],
   },
    {
    "segment": "Hispanic", 
    "filters": [
            {"expression":"bixdemo_ethnicity in [2]"},    
    ],
   },
    {
    "segment": "Asian", 
    "filters": [
            {"expression":"bixdemo_race2 in [4]"},    
    ],
   },
    {
    "segment": "<$80K", 
    "filters": [
            {"expression":"bixdemo_income2 in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"},    
    ],
   },
    {
    "segment": ">$80K", 
    "filters": [
            {"expression":"bixdemo_income2 in [11, 12, 13, 14]"},    
    ],
   },    
    {
    "segment": "Northeast", 
    "filters": [
            {"expression":"bixdemo_region in [1]"},    
    ],
   },
    {
    "segment": "South", 
    "filters": [
            {"expression":"bixdemo_region in [2]"},    
    ],
   },
    
    {
    "segment": "West", 
    "filters": [
            {"expression":"bixdemo_region in [3]"},    
    ],
   },
    {
    "segment": "Midwest", 
    "filters": [
            {"expression":"bixdemo_region in [4]"},    
    ],
   },
    # People from the age group of 27 -42
   {
    "segment": "Gen Z", 
    "filters": [
            { "expression":"bixdemo_agegranular in [1, 2, 3, 4, 5, 6, 7, 8, 9]" },    
    ],
   },
    # People from the age group of 43 - 58 
   {
    "segment": "Millennials", 
    "filters": [
            { "expression":"bixdemo_agegranular in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]" },    
    ],
   },
    # Gen X
    {
    "segment": "Gen X", 
    "filters": [
            { "expression":"bixdemo_agegranular in [26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41]" },    
    ],
   },
    
     # Female gender and has waxed in last 3 months/plans to wax in next 3 months 
    {
    "segment": "WWW", 
    "filters": [
            { "expression":"profiles_us__pdl_gender in [2] and (profiles_us__pdl_persmaintenance_last3m in ['pdl_persmaintenance_last3m_1'] or profiles_us__pdl_persmaintenance_next3m in ['pdl_persmaintenance_next3m_1'])" },    
    ],
   },
    # Male gender and has waxed in last 3 months/plans to wax in next 3 months  
    {
    "segment": "MWW", 
    "filters": [
            { "expression":"profiles_us__pdl_gender in [1] and (profiles_us__pdl_persmaintenance_last3m in [ 'pdl_persmaintenance_last3m_1' ] or profiles_us__pdl_persmaintenance_next3m in ['pdl_persmaintenance_next3m_1'])" },    
    ],
   },
    # EWC aware and has spent atleast $50 personal care/health or beauty products in the past 3 months or has waxed in last 3 months / next 3 months
    {
    "segment": "Core", 
    "filters": [
            { "expression":"(profiles_us__pdl_femhygiene_spend in [4, 5, 6, 7, 8, 9] or (profiles_us__pdl_persmaintenance_last3m in ['pdl_persmaintenance_last3m_1'] or profiles_us__pdl_persmaintenance_next3m in ['pdl_persmaintenance_next3m_1'] )) and profiles_us__opi_score_aware_4b928c96_75e4_11ea_80a7_995726570156_365d in [1]" },    
    ],
   },
    # Not EWC aware and has spent atleast $50 and  personal care/health or beauty products in the past 3 months or has waxed in last 3 months / next 3 months
    {
    "segment": "Opportunity", 
    "filters": [
            { "expression":"(profiles_us__pdl_femhygiene_spend in [4, 5, 6, 7, 8, 9] or (profiles_us__pdl_persmaintenance_last3m in ['pdl_persmaintenance_last3m_1'] or profiles_us__pdl_persmaintenance_next3m in ['pdl_persmaintenance_next3m_1'])) and profiles_us__opi_score_aware_4b928c96_75e4_11ea_80a7_995726570156_365d in [2]" },    
    ],
   },
   # Men who have either waxed in past 3 months or men who are planning to wax for next 3 months 
   {
    "segment": "Men", 
    "filters": [
            { "expression":"profiles_us__pdl_gender in [1] and (profiles_us__pdl_persmaintenance_last3m in ['pdl_persmaintenance_last3m_1'] or profiles_us__pdl_persmaintenance_next3m in ['pdl_persmaintenance_next3m_1'])" },    
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
with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_EWC\\EWC.json", "w") as f:
     json.dump(final_json, f)

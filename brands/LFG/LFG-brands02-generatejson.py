## 2nd Custom brands
import json
from typing import final
import copy

analysisid = "LFGBrandIndexAnalysis"

moving_average = 84   
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'

brands = [
    {"custom_sector_uuid": "b553cd97-f3c5-42ac-939b-adb8b96b7a64"},  
    {"custom_sector_uuid": "c29ad5cc-2240-47dc-b228-9840bd2fc9dd"}, 
    {"custom_sector_uuid": "d969d532-2655-4df6-a610-c57d65b383b8"}, 
    {"custom_sector_uuid": "e2c2e5f9-b286-4420-8950-a4b7c88ee458"},
    {"custom_sector_uuid": "a862fdfc-92aa-4d79-8eea-b1a7916fad7c"}, 
]

filters = [
    {
        "segment": "Total Population",
        "filters": [{ "expression":"bixdemo_gender in [1, 2]"},],
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
            #brand_id = brand["brand_id"]
            #region = brand["region"]
            #sector_id = brand["sector_id"]
            query["id"] = id + "|National"
            entity = {}
            #entity["brand_id"] = brand_id
            #entity["region"] = region
            entity["custom_sector_uuid"] = brand["custom_sector_uuid"]
            query["entity"] = entity
            query["filters"] = _filters
            query["moving_average"] = moving_average
            query["period"] = json.loads(date_period)
            query["metrics_score_types"] = json.loads(metrics_score_types)
            queries.append(query)
        else:
            query = {}
            #brand_id = brand["brand_id"]
            #region = brand["region"]
            #sector_id = brand["sector_id"]
            query["id"] = id
            entity = {}
            #entity["brand_id"] = brand_id
            #entity["region"] = region
            #entity["sector_id"] = sector_id
            entity["custom_sector_uuid"] = brand["custom_sector_uuid"]
            query["entity"] = entity
            query["filters"] = _filters
            query["moving_average"] = moving_average
            query["period"] = json.loads(date_period)
            query["metrics_score_types"] = json.loads(metrics_score_types)
            queries.append(query)

final_json = json.loads(basejson)
final_json["data"]["queries"] = queries
final_json
with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\Documents\\BrandIndex Crossmedia\\XMedia\\BrandIndex_LFG\\LFG_datapipeline2\\LFG_dp2brands02.json", "w") as f:
    json.dump(final_json, f)
####### cluster 1
import json
from typing import final
import copy

analysisid = "Planetfitness2BrandIndexAnalysis"

moving_average = 84 #56
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'


brands = [
    {"brand_id": 1003382, "region": "us", "sector_id": 35},   #Beach body
    {"brand_id": 1003380, "region": "us", "sector_id": 35},   #Jenny Craig
    {"brand_id": 1003378, "region": "us", "sector_id": 35},   #Noom
    {"brand_id": 1001795, "region": "us", "sector_id": 35},   #Nordic Trac
    {"brand_id": 1003381, "region": "us", "sector_id": 35},   #Nutrisystem
    {"brand_id": 1001796, "region": "us", "sector_id": 35},   #Orange Theory
    {"brand_id": 1001628, "region": "us", "sector_id": 35},   #Peloton
    {"brand_id": 1001186, "region": "us", "sector_id": 35},   #Planet fitness
    {"brand_id": 1003379, "region": "us", "sector_id": 35},  #WW(Weight Watchers)
    {"brand_id": 1001187, "region": "us", "sector_id": 35},  #Crunch Gym
    {"brand_id": 1006463, "region": "us", "sector_id": 35},  #EOS Fitness
    {"brand_id": 1006464, "region": "us", "sector_id": 35},  #Lifetime Fitness
]

filters = [
   {"segment": "Total Population", "filters": []},
]


clusters = {
        "cluster1": "inputzipdma_combined2 in [532, 743, 537, 692, 756, 746, 559, 523, 759, 510, 682, 505, 606, 565, 802, 745, 513, 755, 518, 545, 566, 744, 758, 558, 529, 686, 628, 570, 659, 544, 632, 656, 597, 508, 500, 521, 576, 855, 507, 543, 555, 581, 547, 605, 760, 709, 526, 549, 577, 536, 771 ]",
        }

basejson = '{"data": {"id": "##analysisid##","queries":[], "scoring": "total"},"meta":{"version": "v1"}}'
basejson = basejson.replace("##analysisid##", analysisid)

query_json = ""
queries = []
DMAs = {
        # cluster 1
        "Albany - Schenectady - Troy": 532, 
        "Anchorage": 743,
        "Bangor": 537,
        "Beaumont - Port Arthur" : 692,
        "Billings" : 756,
        "Biloxi - Gulfport" : 746,
        "Bluefield - Beckley - Oak Hill":559,
        "Burlington - Plattsburgh": 523,
        "Cheyenne - Scottsbluff": 759,
        "Cleveland" : 510 ,
        "Davenport - Rock Island - Moline": 682,
        "Detroit" : 505,
        "Dothan" : 606,
        "Elmira" : 565,
        "Eureka" : 802,
        "Fairbanks" : 745,
        "Flint - Saginaw - Bay City" : 513,
        "Great Falls":755,
        "Greensboro - High Point - Winston-Salem" : 518,
        "Greenville - New Bern - Washington" : 545,
        "Harrisburg - Lancaster - Lebanon - York": 566,
        "Honolulu":744,
        "Idaho Falls - Pocatello" : 758,
        "Lima": 558,
        "Louisville":529,
        "Mobile - Pensacola - Fort Walton Beach":686,
        "Monroe - El Dorado": 628,
        "Florence - Myrtle Beach": 570,
        "Nashville": 659,
        "Norfolk - Portsmouth - Newport News": 544,
        "Paducah - Cape Girardeau - Harrisburg - Mt Vernon": 632,
        "Panama City" : 656,
        "Parkersburg" : 597, 
        "Pittsburgh" : 508,
        "Portland - Auburn":500,
        "Providence - New Bedford": 521,
        "Salisbury": 576,
        "Santa Barbara - Santa Maria - San Luis Obispo":855,
        "Savannah":507,
        "Springfield - Holyoke":543,
        "Syracuse" : 555,
        "Terre Haute" : 581,
        "Toledo": 547,
        "Topeka": 605,
        "Twin Falls": 760,
        "Tyler - Longview - Lufkin - Nacogdoches":709,
        "Utica":526,
        "Watertown": 549,
        "Wilkes Barre - Scranton": 577,
        "Youngstown": 536,
        "Yuma - El Centro": 771
    }
for filter in filters:
        
    id = filter["segment"]
    _filters = filter["filters"]
    for brand in brands:
        if clusters:
            for cluster in clusters:
                # deepcopy in order to retain original filters
                # without polluting from each sub_reg specific append below
                _filters_clusters = copy.deepcopy(_filters)
                query = {}
                _filters_clusters.append(
                    {"expression": clusters[cluster] }
                )
                brand_id = brand["brand_id"]
                region = brand["region"]
                sector_id = brand["sector_id"]
                query["id"] = id + "|" + cluster
                entity = {}
                entity["brand_id"] = brand_id
                entity["region"] = region
                entity["sector_id"] = sector_id
                query["entity"] = entity
                query["filters"] = _filters_clusters
                query["moving_average"] = moving_average
                query["period"] = json.loads(date_period)
                query["metrics_score_types"] = json.loads(metrics_score_types)
                queries.append(query)
        if DMAs:
            for dma in DMAs:
                _filters_dma = copy.deepcopy(_filters)
                query = {}
                _filters_dma.append(
                    {"expression": "inputzipdma_combined2 in [{}]".format(DMAs[dma])}
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

with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_Planetfitness2\\Planetfitness2_cluster1.json", "w") as f:
    json.dump(final_json, f)

####cluster 3##################################################################
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
    
         "cluster3":  "inputzipdma_combined2 in [790, 512, 716, 821, 630, 519, 868, 515, 546, 535, 600, 765, 516, 866, 571, 670, 509, 563, 567, 533, 766, 691, 561, 616, 557, 702, 642, 643, 551, 541, 651, 813, 622, 633, 820, 560, 538, 610, 612, 881, 619, 530, 626, 625, 548, 810 ]",
}

basejson = '{"data": {"id": "##analysisid##","queries":[], "scoring": "total"},"meta":{"version": "v1"}}'
basejson = basejson.replace("##analysisid##", analysisid)

query_json = ""
queries = []
DMAs = {
        # cluster 3
        "Albuquerque - Santa Fe": 790,
        "Baltimore": 512,
        "Baton Rouge": 716,
        "Bend-Or": 821,
        "Birmingham": 630,
        "Charleston-Sc": 519,
        "Chico - Redding": 868,
        "Cincinnati": 515,
        "Columbia-Sc": 546,
        "Columbus-Ga": 522,
        "Columbus-Oh": 535,
        "Corpus Christi": 600,
        "El Paso": 765,
        "Erie": 516,
        "Fresno - Visalia": 866,
        "Fort Myers - Naples": 571,
        "Fort Smith - Fayetteville - Springdale - Rogers": 670,
        "Fort Wayne": 509,
        "Grand Rapids - Kalamazoo - Battle Creek": 563,
        "Greenville - Spartanburg - Asheville - Anderson": 567,
        "Hartford - New Haven": 533,
        "Helena": 766,
        "Huntsville - Decatur - Florence": 691,
        "Jacksonville - Brunswick": 561,
        "Kansas City": 616,
        "Knoxville": 557,
        "La Crosse - Eau Claire": 702,
        "Lafayette-La": 642,
        "Lake Charles": 643,
        "Lansing": 551,
        "Lexington": 541,
        "Lubbock": 651,
        "Medford - Klamath Falls": 813,
        "New Orleans": 622,
        "Odessa - Midland": 633,
        "Portland-Or": 820,
        "Raleigh - Durham": 560,
        "Rochester - NY": 538,
        "Rockford": 610,
        "Shreveport": 612,
        "Spokane": 881,
        "Springfield-Mo": 619,
        "Tallahassee - Thomasville": 530,
        "Victoria": 626,
        "Waco - Temple - Bryan": 625,
        "West Palm Beach - Fort Pierce": 548,
        "Yakima - Pasco - Richland - Kennewick": 810,

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
#             # For all national markets
#             query = {}
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

with open("C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_Planetfitness2\\Planetfitness2_cluster3.json", "w") as f:
    json.dump(final_json, f)

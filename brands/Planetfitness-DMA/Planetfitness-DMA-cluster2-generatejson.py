############### cluster 2 ########################3
import json
from typing import final
import copy

analysisid = "Planetfitness2BrandIndexAnalysis"

moving_average = 84  # 56
scoring = "total"
metrics_score_types = '{"index": "net_score","buzz": "net_score","impression": "net_score","quality": "net_score","value": "net_score","reputation": "net_score","satisfaction": "net_score","recommend": "net_score","aided": "net_score","attention": "net_score","adaware": "net_score","wom": "net_score","consider": "net_score","likelybuy": "net_score","current_own": "net_score","former_own": "net_score"}'
date_period = '{"end_date": {"date": "###end_date###"},"start_date": {"date": "###start_date###"}}'


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
    {"segment": "Total Population", "filters": []},
]


clusters = {
    "cluster2": "inputzipdma_combined2 in [525, 644, 634, 520, 800, 736, 514, 767, 637, 648, 564, 517, 575, 598, 752, 673, 542, 679, 801, 649, 724, 773, 658, 636, 569, 710, 527, 639, 574, 603, 722, 693, 737, 640, 711, 617, 613, 687, 698, 650, 652, 804, 675, 764, 573, 611, 862, 770, 661, 641, 657,624, 588, 638, 609, 539, 540, 531, 789, 705, 554, 627, 678, 550, 596]",
}

basejson = '{"data": {"id": "##analysisid##","queries":[], "scoring": "total"},"meta":{"version": "v1"}}'
basejson = basejson.replace("##analysisid##", analysisid)

query_json = ""
queries = []
DMAs = {
    # cluster 2
    "Albany-Ga": 525,
    "Alexandria-La": 644,
    "Amarillo": 634,
    "Augusta": 520,
    "Bakersfield": 800,
    "Bowling Green": 736,
    "Buffalo": 514,
    "Casper - Riverton": 767,
    "Cedar Rapids - Waterloo - Dubuque": 637,
    "Champaign - Springfield - Decatur": 648,
    "Charleston - Huntington": 564,
    "Charlotte": 517,
    "Chattanooga": 575,
    "Clarksburg - Weston": 598,
    "Colorado Springs - Pueblo": 752,
    "Columbus - Tupelo - West Point": 673,
    "Dayton": 542,
    "Des Moines - Ames": 679,
    "Eugene": 801,
    "Evansville": 649,
    "Fargo - Valley City": 724,
    "Grand Junction - Montrose": 773,
    "Green Bay - Appleton": 658,
    "Harlingen - Weslaco - Brownsville - Mcallen": 636,
    "Harrisonburg": 569,
    "Hattiesburg - Laurel": 710,
    "Indianapolis": 527,
    "Jackson-Tn": 639,
    "Johnstown - Altoona": 574,
    "Joplin - Pittsburg": 603,
    "Lincoln - Hastings - Kearney": 722,
    "Little Rock - Pine Bluff": 693,
    "Mankato": 737,
    "Memphis": 640,
    "Meridian": 711,
    "Milwaukee": 617,
    "Minneapolis - Saint Paul": 613,
    "Minot - Bismarck - Dickinson": 687,
    "Montgomery - Selma": 698,
    "Oklahoma City": 650,
    "Omaha": 652,
    "Palm Springs": 804,
    "Peoria - Bloomington": 675,
    "Rapid City": 764,
    "Roanoke - Lynchburg": 573,
    "Rochester - Mason City - Austin": 611,
    "Sacramento - Stockton - Modesto": 862,
    "Salt Lake City": 770,
    "San Angelo": 661,
    "San Antonio": 641,
    "Sherman-Tx - Ada-Ok": 657,
    "Sioux City": 624,
    "South Bend - Elkhart": 588,
    "Saint Joseph": 638,
    "Saint Louis": 609,
    "Tampa - Saint Petersburg": 539,
    "Traverse City - Cadillac": 540,
    "Tri-Cities-Tn-Va": 531,
    "Tucson - Sierra Vista": 789,
    "Wausau - Rhinelander": 705,
    "Wheeling - Steubenville": 554,
    "Wichita Falls - Lawton": 627,
    "Wichita - Hutchinson": 678,
    "Wilmington": 550,
    "Zanesville": 596,
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
                _filters_clusters.append({"expression": clusters[cluster]})
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

with open(
    "C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_Planetfitness2\\Planetfitness2_cluster2.json",
    "w",
) as f:
    json.dump(final_json, f)

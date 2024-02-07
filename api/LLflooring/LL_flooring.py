from operator import index
import json
import os
import datetime
import configparser
import ast
import pandas as pd
import io
#import boto3
from BIConnector import *
from helper import *
import time

wait_time_each_api_call_in_sec = 30
max_queries_single_go = 100

def configurations_variable():
    config_json = {}
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    config_json["S3_BUCKET"] = Config.get("PATH", "S3_BUCKET")
    config_json["BRANDSINDEX_BRANDS"] = Config.get("BRANDS", "BRANDSINDEX_BRANDS")
    return config_json

event=None
context = "local"
response = ""
brands_executed_successfully = ""
brands_failed_executing = ""

execute_local = False
if context == "local":
    execute_local = True

# Calling brandindex_api_data function. Provide currentdate-1 as function parameter.
output_status_message("BrandIndex API pull started")

#end_date = datetime.date.today() - datetime.timedelta(days=2)
#start_date = datetime.date.today() - datetime.timedelta(days=7)

end_date = datetime.date(2023, 8, 1)
start_date = datetime.date(2023, 7, 26)

output_status_message(
    "start date and end date for this run are {} and {}".format(
        start_date, end_date
    )
)

configs = configurations_variable()

if execute_local:
    username = ""
    password = ""
else:
    username = os.environ["email"]
    password = os.environ["password"]

output_status_message("Authenticating")
session = authenticate(username, password)
output_status_message("Authenticated")

regions = ["us"]
all_sectors = []
for region in regions:
    output_status_message("Getting sectors for region {}".format(region))
    sectrs = get_sectors(session, region)
    sec_data = sectrs.json()["data"]
    for attribute in sec_data:
        all_sectors.append(sec_data[attribute])
df_all_sectors = pd.DataFrame(all_sectors)

brands = ast.literal_eval(configs["BRANDSINDEX_BRANDS"])
#s3 = boto3.client("s3")

for brand_item in brands:
    brand_data = json.loads(str(brand_item))
    brand = brand_data["name"]
    is_roll_up = brand_data["roll_up"]
    brand_aggregation = brand_data["brand_aggregation"]
    has_dma = brand_data["has_dma"]
    has_sub_region = brand_data["has_sub_region"]
    volumn_percent = brand_data["volumn_percent"]
    
    
query_index = {}
sectors_and_regions = []

output_status_message(
    "Running brand index analysis for the brand : {}".format(brand)
)
path1 = "C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_LLflooring\\"
with open(path1 + "LL_flooring.json", "r") as f:
    content = f.read()
    content = content.replace(
        "###start_date###", start_date.strftime("%Y-%m-%d")
    ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))

data = json.loads(content)
index = 0

for query in data["data"]["queries"]:
    query_index[index] = query["moving_average"]
    index = index + 1
    sectors_and_regions.append(
        {
            "region": query["entity"]["region"],
            "sector_id": query["entity"]["sector_id"],
        }
    )

sectors_and_regions = pd.DataFrame(sectors_and_regions)
sectors_and_regions = sectors_and_regions.drop_duplicates(
    subset=["sector_id"], keep="first"
)

sector_brands = []
df_sector_brands = pd.DataFrame()
for index, row in sectors_and_regions.iterrows():
    output_status_message(
        "Getting brands for sector {}".format(row["sector_id"])
    )
    sectrs = get_brands(session, row["sector_id"], row["region"])
    sec_data = sectrs.json()["data"]
    for attribute in sec_data:
        sector_brands.append(sec_data[attribute])
df_sector_brands = pd.DataFrame(sector_brands)


total_queries = data["data"]["queries"]
df = pd.DataFrame()

for queries in chunks(total_queries, max_queries_single_go):
    output_status_message(
        "sleeping in seconds, {}".format(wait_time_each_api_call_in_sec)
    )
    time.sleep(wait_time_each_api_call_in_sec)
    data["data"]["queries"] = queries
    response = run_analysis(session, data)
    
    temp_frame = pd.read_csv(
        io.StringIO(response.content.decode("utf-8"))
    )
    df = pd.concat([df,temp_frame])

output_status_message(
    "Successfully ran brand index analysis for the brand : {}".format(
        brand
    )
)

print('deepanshu',df)
df = enrich_data_frame(
    df, query_index, df_all_sectors, df_sector_brands, has_dma
)


if is_roll_up == "true":
    df = aggregate_weekly(df)

if has_dma == "true":
    df[["segment", "dma"]] = df["segment"].str.split("|", expand=True)

if has_sub_region == "true":
    df[["segment", "geo"]] = df["segment"].str.split("|", expand=True)

if volumn_percent=="true":
    df = sentiment_percentage_cols(df)
    print('dataframe', df)

if execute_local:
    outputname = "{brand}_{start_date}_{end_date}.csv".format(
        brand=brand, start_date=start_date, end_date=end_date
    )
    df.to_csv(path1  + "data\\" +  outputname, index=False)
    #df.to_csv(path1 +"invesco_wrapper_df1.csv", index=False)
else:
    csv_buffer = io.StringIO()



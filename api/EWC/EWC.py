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

#end_date = datetime.date.today() - datetime.timedelta(days=5)
#start_date = datetime.date.today() - datetime.timedelta(days=11)

end_date = datetime.date(2023, 8, 13)
start_date = datetime.date(2023, 8, 7)

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
    has_dma = brand_data["has_dma"]
    has_sub_region = brand_data["has_sub_region"]
    volumn_percent = brand_data["volumn_percent"]
    
    
query_moving_average = {}
sectors_and_regions = []

output_status_message(
    "Running brand index analysis for the brand : {}".format(brand)
)

path1="C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\BrandIndex_EWC\\"
with open(path1 + "EWC.json", "r") as f:
    content = f.read()
    content = content.replace(
        "###start_date###", start_date.strftime("%Y-%m-%d")
    ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))

data = json.loads(content)
index = 0

for query in data["data"]["queries"]:
    query_moving_average[index] = query["moving_average"]
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

df = enrich_data_frame(
    df, query_moving_average, df_all_sectors, df_sector_brands, has_dma
)

df["positives"] = df["positives"] * df["volume"] / 100
df["negatives"] = df["negatives"] * df["volume"] / 100
df["neutrals"] = df["neutrals"] * df["volume"] / 100
df["unaware"] = df["volume"] - (df["positives"] + df["negatives"] + df["neutrals"])
df["unaware"] = df["unaware"].mask(df["unaware"] < 1, 0)

if is_roll_up == "true":
    df = aggregate_weekly(df)

if has_dma == "true":
    df[["segment", "dma"]] = df["segment"].str.split("|", expand=True)

if has_sub_region == "true":
    df[["segment", "geo"]] = df["segment"].str.split("|", expand=True)

if volumn_percent=="true":
    df = sentiment_percentage_cols(df)

df["Report Name"] = "EWC"

df.rename(columns={'brand_name':'brand',
                    'positives': 'positive_yes',
                    'negatives': 'negative_no',
                    "neutrals": "neutral",
                    "segment": "Segment BI (API)",
                    "moving_average": "Moving Average",
                    'sector_id':'Sector Code',
                    'sector_name':'Sector Name',
                    }, inplace=True)


update_names = [
    "F" + name.split("+", 1)[1].strip()
    for name in df[
        df["Segment BI (API)"].str.split("+", 1, expand=True)[0] == "Female "
    ]["Segment BI (API)"]
]

df.loc[
    df["Segment BI (API)"].str.split("+", 1, expand=True)[0] == "Female ",
    "Segment BI (API)",
] = update_names


df = df[['date',"brand",'brand_id', 'region', 'metric', 'volume','score', 'positive_yes', 'negative_no', 'neutral',
     'unaware','Segment BI (API)','Moving Average','Sector Code', 'Sector Name', 'Report Name']]


if execute_local:
    outputname = "Brandindex_{brand}_{start_date}_{end_date}.csv".format(
        brand=brand, start_date=start_date, end_date=end_date
    )
    df.to_csv(path1  +  outputname, index=False)
    #df.to_csv(path1 +"EWC_wrapper_df1.csv", index=False)
else:
    csv_buffer = io.StringIO()

















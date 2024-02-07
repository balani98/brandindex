
# In LFG datapipeline 2 we have taken only 5 new demos as per request from
# mellisa and also need to create and schedule new pipleline for these new
# demos in Lambda.
# 1.	Affluent Aspirers 30-49
# 2.	Affluent Seekers 50-69
# 3.	Mass Middle Aspirers 25-44
# 4.	Mass Middle Aspirers 45+
# 5.	Total Population


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
from datetime import timedelta, date
import numpy as np

wait_time_each_api_call_in_sec = 5
max_queries_single_go = 50


def configurations_variable():
    config_json = {}
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    config_json["S3_BUCKET"] = Config.get("PATH", "S3_BUCKET")
    config_json["BRANDSINDEX_BRANDS"] = Config.get("BRANDS", "BRANDSINDEX_BRANDS")
    return config_json




response = ""
brands_executed_successfully = ""
brands_failed_executing = ""
context = "local"
execute_local = False
if context == "local":
    execute_local = True
# Calling brandindex_api_data function. Provide currentdate-1 as function parameter.
output_status_message("BrandIndex API pull started")
try:
    end_date = datetime.date.today() - datetime.timedelta(days=2)
    start_date = end_date.replace(day=1) 
    
    end_date = datetime.date(2023, 12, 31)
    start_date = datetime.date(2023, 12, 1)
    
    output_status_message(
        "start date and end date for this run are {} and {}".format(
            start_date, end_date))
    
    configs = configurations_variable()
    
    if execute_local:
        username = "YGAPI@xmedia.com"
        password = "YouGov123"
    else:
        #username = os.environ["email"]
        #password = os.environ["password"]
        username = ""
        password = ""
    
    
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
    
    output_status_message(
        "Running brand index analysis for the brand : {}".format(brand)
    )
    path1 = "C:\\Users\\deepanshu.balani\\OneDrive - Nabler Web Solutions Pvt. Ltd\\Documents\\BrandIndex Crossmedia\\XMedia\\BrandIndex_LFG\\LFG_datapipeline2\\"
    def LFG_dfs(json_filename):
        query_index = {}
        sectors_and_regions = []
        # response = s3.get_object(
        #         Bucket=configs["S3_BUCKET"],
        #         Key="lambda_config/{}.json".format(json_filename),
        #     )
        with open(path1 + json_filename, "r") as f:
            content = f.read()
            content = content.replace(
            "###start_date###", start_date.strftime("%Y-%m-%d")
            ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))
            data = json.loads(content)
            #index = 0
            response = run_analysis(session, data)
            temp_frame = pd.read_csv(
                io.StringIO(response.content.decode("utf-8"))
            )
            return temp_frame

    #print(response)
    print("This is check 2")
    df1 = LFG_dfs("LFG_dp2brands01.json")
    print('1 done')
    df2 = LFG_dfs("LFG_dp2brands02.json")
    print('2 done')
    df3 = LFG_dfs("LFG_dp2brands03.json")
    print('3 done')
    print("This is check 3")
    
    df = pd.concat([df1,df3])
    #df=df1
    print(df.columns)
    print(df1.shape)
    print(df2.shape)
    print(df3.shape)
    print(df.shape)

    df.rename(columns={"date": "date", 
                "analysis_id": "Report Name",
                "metric": "metric",
                "volume": "volume",              
                "score": "score",
                "positives": "positive_yes",
                "negatives": "negative_no",
                "neutrals": "neutral",
                "query_id": "Demo",
                "custom_sector_uuid": "Sector Name"
                },
        inplace=True)

    df = df[['date', 'Report Name','metric',  'volume', 'score', 'positive_yes',
    'negative_no', 'neutral', 'Demo','Sector Name', 'region', 'sector_id', 'brand_id']]
    # @CALLOUT -> work around for now since moving average is hardcoded
    # to whatever the first value seen is
    df["Moving Average"] = 12
    df["Sector Name"].replace(
        {
            "b553cd97-f3c5-42ac-939b-adb8b96b7a64": "LFG Insurance Competitor",
            "c29ad5cc-2240-47dc-b228-9840bd2fc9dd": "LFG FA Competitor Sector",
            "d969d532-2655-4df6-a610-c57d65b383b8": "LFG-Tier 1",
            "e2c2e5f9-b286-4420-8950-a4b7c88ee458": "LFG-Tier 2",
            "a862fdfc-92aa-4d79-8eea-b1a7916fad7c": "LFG-Tier 3",
        },    inplace=True,)
    df.loc[:, "Sector Code"] = df["Sector Name"]
    df.loc[df["Sector Name"] == "LFG Insurance Competitor", "Sector Code"] = -175
    df.loc[df["Sector Name"] == "LFG FA Competitor Sector", "Sector Code"] = -176
    df.loc[df["Sector Name"] == "LFG-Tier 3", "Sector Code"] = -179
    df.loc[df["Sector Name"] == "LFG-Tier 2", "Sector Code"] = -178
    df.loc[df["Sector Name"] == "LFG-Tier 1", "Sector Code"] = -177
    df.loc[(df["brand_id"]  == 27009) | (df["brand_id"] == 27024)
            | (df["brand_id"]  == 1001752) | (df["brand_id"] == 1002056)
            | (df["brand_id"]  == 1000341) | (df["brand_id"] == 1001310)
            | (df["brand_id"]  == 1003688) | (df["brand_id"] == 1004059)
            | (df["brand_id"]  == 27021) | (df["brand_id"] == 1003755), "Sector Code"] = 27
    
    df.loc[(df["brand_id"]  == 27026) | (df["brand_id"] == 25004)
            | (df["brand_id"]  == 25011) | (df["brand_id"] == 25013)
            | (df["brand_id"]  == 25016) | (df["brand_id"] == 25018)
            | (df["brand_id"]  == 25019) | (df["brand_id"] == 1001172)
            | (df["brand_id"]  == 1003515) | (df["brand_id"] == 25017)
            | (df["brand_id"]  == 25021) | (df["brand_id"] == 25021)
            | (df["brand_id"]  == 1003770) | (df["brand_id"] == 27015)
            
            | (df["brand_id"]  == 25003) | (df["brand_id"] == 25007)
            | (df["brand_id"]  == 1001136)| (df["brand_id"]  == 1005729) , "Sector Code"] = 25
    ##27	
    df["brand_id"].replace(
        {
        27009:"Fidelity", 
        27024: "Vanguard",
        1001752: "Nationwide",
        1002056: "Voya Financial",
        1000341: "Prudential",
        1001310: "TIAA",
        1003688: "Lincoln Financial",
        1004059: "Brighthouse Financial",
        27021: "T. Rowe Price",           
        1003755: "Empower Retirement"
        },
            inplace=True,
    )
    ##25
    df["brand_id"].replace(
        {
        27026:  "Guardian",
        25004:  "AIG",
        25011:"The Hartford",
        25013:"John Hancock",
        25016:  "MetLife",
        25018:"New York Life",
        25019:"Northwestern Mutual",
        1001172:"Pacific Life",
        1003515:  "Transamerica",
        25017:"Nationwide",
        25021:  "Prudential",
        1003770:  "Lincoln Financial",
        27015:"Mass Mutual",
        25003:"Aflac",
        25007:  "Cigna",
        1001136:"Athene",  
        1005729: "Corebridge Financial" 
        }, 
        inplace=True
    )
    df.loc[df["Sector Code"] == 27, "Sector Name"] = "Financial Services-Investment Advisors"
    df.loc[df["Sector Code"] == 25, "Sector Name"] = "Insurance" 
        
    df.rename(columns={"brand_id": "brand"}, inplace=True)
    print("brands",df["brand"])
    df["positive_yes"] = df["positive_yes"] * df["volume"] / 100
    df["negative_no"] = df["negative_no"] * df["volume"] / 100
    df["neutral"] = df["neutral"] * df["volume"] / 100
    df["unaware"] = df["volume"] - (
        df["positive_yes"] + df["negative_no"] + df["neutral"]
    )
    df.columns
    df.drop(columns=["region","sector_id"], inplace=True)
    
    df = df.loc[~((df['brand'] =="Aflac") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="AIG") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Athene") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Cigna") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Guardian") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="John Hancock") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Mass Mutual") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="MetLife") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="New York Life") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Northwestern Mutual") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Pacific Life") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="The Hartford") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Transamerica") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    df = df.loc[~((df['brand'] =="Corebridge Financial") &  (df['Sector Name'] =="Financial Services-Investment Advisors"))]
    
    df = df.loc[~((df['brand'] =="Brighthouse Financial") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Empower Retirement") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Fidelity") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Lincoln Financial") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Nationwide") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Prudential") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="T. Rowe Price") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="TIAA") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Vanguard") &  (df['Sector Name'] =="Insurance"))]
    df = df.loc[~((df['brand'] =="Voya Financial") &  (df['Sector Name'] =="Insurance"))]
   
    #df = df.loc[(df['Demo'] == 'Total Population')]
    #df5['Sector Name'] = 'Category' 
    #df5['brand'] = ''
    #df5['Sector Code'] = '' 
    
    #df = pd.concat([df,df4, df5])
    
    
    df["date"] = min(df["date"])
    df = df.groupby(
            [
                "date",
                df['brand'].fillna(''),
                "Demo",
                "metric",
                "Moving Average",
                df["Sector Code"].fillna(''),
                "Sector Name",  
                "Report Name",
            ]
        ).agg(
            {
                "volume": "sum",
                "score": "mean",
                "positive_yes": "sum",
                "negative_no": "sum",
                "neutral": "sum",
                "unaware": "sum",
            }
        ).reset_index()
     
    df['tier_category'] = 'N/A'
    df['tier_category'] = np.where((df['Demo'] != 'Total Population'), 'N/A', df['tier_category'])
    df['tier_category'] = np.where((df['Demo'] == 'Total Population') & (df['metric'] != 'aided'), 'N/A', df['tier_category'])
    df['tier_category'] = np.where((df['metric'] == 'aided') & (df['Demo'] == 'Total Population') & (df['score'] >= 60.0), 'Tier 1', df['tier_category'])
    df['tier_category'] = np.where((df['metric'] == 'aided') & (df['Demo'] == 'Total Population') & ((df['score'] >= 30.0) & (df['score'] < 60.0)), 'Tier 2', df['tier_category'])
    df['tier_category'] = np.where((df['metric'] == 'aided') & (df['Demo'] == 'Total Population') & (df['score'] < 30.0), 'Tier 3', df['tier_category'])
    #brands_to_be_updated_with_tier = ['AIG', 'Aflac', 'Cigna', 'Empower Retirement', 'Fidelity', 'Guardian', 'John Hancock', 'Mass Mutual', 'MetLife', 'Nationwide', 'New York Life', 'Northwestern Mutual', 'Pacific Life', 'Prudential','T. Rowe Price','The Hartford','TIAA','Transamerica','Vanguard','Voya Financial']
    brands_to_be_updated_with_tier = df['brand'].unique()
    for brand_tier in brands_to_be_updated_with_tier:                                  
        update_condition = ((df['brand'] == brand_tier) & (df['metric'] != 'aided') & (df['Demo'] == 'Total Population'))
        retrieve_condition = ((df['brand'] == brand_tier) & (df['metric'] == 'aided') & (df['Demo'] == 'Total Population'))
        # Retrieve the tier_category value based on the retrieve_condition
        tier_category_value = df.loc[retrieve_condition, 'tier_category'].values[0] if df.loc[retrieve_condition].shape[0] else np.nan
        # Update 'tier_category' with tier_category_value where update_condition is True
        df['tier_category'] = np.where(update_condition, tier_category_value, df['tier_category'])
        # making the new sector  category  for aggregation accross brands 
    
    df = df[['date','brand','metric','volume', 'score',  'positive_yes',
            'negative_no', 'neutral',"unaware", 'Demo', 'Moving Average','Sector Code', 'Sector Name','tier_category',
            'Report Name']]
    print("here is lfg2 brand got completed")
    # csv_buffer = io.StringIO()
    # df.to_csv(csv_buffer, index=False)
    
    # lfg_filename= 'BrandIndex_LFG2_' + start_date.strftime("%b") + str(start_date.year)  + '.csv'
    # s3 = boto3.client("s3")
    # s3.put_object(
    #     Body=csv_buffer.getvalue(),
    #     Bucket=configs["S3_BUCKET"],
    #     Key="BI_"
    #     + brand
    #     + "/{filename}".format(
    #         filename=lfg_filename
    #     ),
    # )
    if execute_local:
        outputname = "{brand}_{start_date}_{end_date}.csv".format(
        brand=brand, start_date=start_date, end_date=end_date
        )
        df.to_csv(path1  + "data\\" +  outputname, index=False)
    #df.to_csv(path1 +"invesco_wrapper_df1.csv", index=False)
    else:
        csv_buffer = io.StringIO()
    output_status_message(
        "analysis successfully finished for brand : " + brand
    )
    brands_executed_successfully = (
        brands_executed_successfully + "," + brand
    )
    ###############################
    output_status_message("BrandIndex API pull completed")
    response = (
        "Brand Index API pull ran successfully !. Details : success "
        + brands_executed_successfully
        + "\n. Failed : "
        + brands_failed_executing
    )
except Exception as ex:
    output_status_message(ex)
    raise ex



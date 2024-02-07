from operator import index
import json
import os
import datetime
import configparser
import ast
import pandas as pd
import io
import boto3
from BIConnector import *
from helper import *
import time
import logging 
import shutil
#from datetime import datetime
wait_time_each_api_call_in_sec = 5
max_queries_single_go = 50

def create_logger_object(log_level):
	"""
	create_logger_object : Creating the Log Object

	This function creates a log object with specified logging level, to be used in rest of the script.

	Args:
		log_level (String): Log Level to initiate the logger object with

	Returns:
		Object : Logger object with specified logging level.
	"""
	try:
		print("making log")
		logger = logging.getLogger(__name__)

		logger_dict = {'debug': logging.DEBUG,
					   'info': logging.INFO,
					   'warning': logging.WARNING,
					   'error': logging.ERROR,
					   'critical': logging.CRITICAL}

		for key, value in logger_dict.items():
			if key == log_level.lower():
				logger.setLevel(value)

		if os.path.exists('Logs'):
			shutil.rmtree('Logs')
		
		#if not os.path.exists('Logs'):
		os.makedirs('Logs')

		formatter = logging.Formatter(
			'%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s')
		file_name_without_path=datetime.datetime.now().strftime('deployment_%Y-%m-%d_%H-%M.log')
		file_name = os.path.join(
			'Logs/', datetime.datetime.now().strftime('deployment_%Y-%m-%d_%H-%M.log'))
		file_handler = logging.FileHandler(file_name)
		file_handler.setFormatter(formatter)

		logger.addHandler(file_handler)

		return logger,file_name,file_name_without_path

	except Exception as exp_logger:
		print('Cannot create logger object', str(exp_logger))
		raise exp_logger
     
LOG_LEVEL = 'debug'

def configurations_variable():
    config_json = {}
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    config_json["S3_BUCKET"] = Config.get("PATH", "S3_BUCKET")
    config_json["BRANDSINDEX_BRANDS"] = Config.get("BRANDS", "BRANDSINDEX_BRANDS")
    return config_json


def lambda_handler(event, context):
    logger,file_name,file_name_without_path = create_logger_object(log_level=LOG_LEVEL)
    response = ""
    brands_executed_successfully = ""
    brands_failed_executing = ""

    execute_local = False
    if context == "local":
        execute_local = True
    # Calling brandindex_api_data function. Provide currentdate-1 as function parameter.
    output_status_message("BrandIndex API pull started")
    try:
        #end_date = datetime.date.today() - datetime.timedelta(days=1)
        #start_date = end_date.replace(day=1) 
        
        end_date = datetime.datetime.strptime(event['end_date'], "%Y/%m/%d").date()
        start_date = datetime.datetime.strptime(event['start_date'], "%Y/%m/%d").date()
        
        #end_date = datetime.date(2022, 4, 30)
        #start_date = datetime.date(2022, 4, 1)
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
        s3 = boto3.client("s3")

        for brand_item in brands:
            try:
                brand_data = json.loads(str(brand_item))
                brand = brand_data["name"]
                is_roll_up = brand_data["roll_up"]
                has_dma = brand_data["has_dma"]
                has_sub_region = brand_data["has_sub_region"]   ########
                volumn_percent = brand_data["volumn_percent"]   ########

               
                output_status_message(
                    "Running brand index analysis for the brand : {}".format(brand)
                )
                def process_planetfitness_df(filename):
                    content = {}
                    query_moving_average = {}
                    sectors_and_regions = []

                    if execute_local:
                        with open(brand + ".json", "r") as f:
                            content = f.read()
                            content = content.replace(
                                "###start_date###", start_date.strftime("%Y-%m-%d")
                            ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))
                    else:
                        response = s3.get_object(
                            Bucket=configs["S3_BUCKET"],
                            Key="lambda_config/{}.json".format(filename),
                        )
                        content = response["Body"].read().decode("utf-8")
                        content = content.replace(
                            "###start_date###", start_date.strftime("%Y-%m-%d")
                        ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))
                        
                    data = json.loads(content)
    
                    index = 0
                    for query in data["data"]["queries"]:
                        query_moving_average[query["id"]] = query["moving_average"]
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
                        ##########################################
                        # Retry in case of API Failure or Incomplete data.
                        response_val = False
                        itr = 1
                        while itr <= 3:
                            if int(str(response)[11:14]) == 200:  
                                response_val = True
                                break
                            elif int(str(response)[11:14]) != 200:
                                time.sleep(60)
                                print("####### Response is not 200 run again ###########")
                                response = run_analysis(session, data)  
                            else:
                                pass
                            itr += 1
                        if response_val != True:
                            raise Exception("Sorry, Max tries exceeded ")
                    
                        ##########################################
                        temp_frame = pd.read_csv(
                            io.StringIO(response.content.decode("utf-8"))
                        )
                        df = pd.concat([df, temp_frame])
                    
                    output_status_message(
                        "Successfully ran brand index analysis for the brand : {} , {}".format(
                            brand , filename
                        )
                    )
                    logger.debug( "Successfully ran brand index analysis for the brand : {} , {}".format(
                            brand , filename
                        ))
                    df = enrich_data_frame(
                        df, query_moving_average, df_all_sectors, df_sector_brands, has_dma
                    )
                    return df

                df_cluster1 = process_planetfitness_df('PlanetFitness2_cluster1')
                df_cluster2 = process_planetfitness_df('PlanetFitness2_cluster2')
                df_cluster3 = process_planetfitness_df('PlanetFitness2_cluster3')
                df_cluster4 = process_planetfitness_df('PlanetFitness2_cluster4')
                df_cluster5 = process_planetfitness_df('PlanetFitness2_cluster5')
                df_cluster6 = process_planetfitness_df('PlanetFitness2_cluster6')
               
                df = pd.concat([df_cluster1, df_cluster2, df_cluster3, df_cluster4, df_cluster5, df_cluster6])
                if is_roll_up == "true":
                    df = aggregate_weekly(df)

                if has_dma == "true":
                    df[["segment", "dma"]] = df["segment"].str.split("|", expand=True)
                
                if has_sub_region == "true":
                    df[["segment", "geo"]] = df["segment"].str.split("|", expand=True)
                
                if volumn_percent=="true":
                    df = sentiment_percentage_cols(df)

                if execute_local:
                    outputname = "{brand}_{start_date}_{end_date}.csv".format(
                        brand=brand, start_date=start_date, end_date=end_date
                    )
                    df.to_csv(outputname, index=False)
                else:
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    print("deepanshu1")
                    #df.to_csv('output.csv.gz', compression='gzip', index=False)
                    push_to_s3(
                        csv_buffer, configs["S3_BUCKET"], brand, start_date, end_date
                    )
                    print('deepanshu3')
                output_status_message(
                    "analysis successfully finished for brand : " + brand
                )
                logger.debug("analysis successfully finished for brand : " + brand)
                brands_executed_successfully = (
                    brands_executed_successfully + "," + brand
                )                    
                    
            ##############
            except Exception as e:
                output_status_message(
                    "analysis not finished for brand due to error : " + brand_item
                )
                logging.error("analysis not finished for brand due to error : " + brand_item)
                brands_failed_executing = brands_failed_executing + "," + brand_item
                output_status_message(e)
                
                ################################
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
    return {"statusCode": 200, "body": json.dumps(response)}


#lambda_handler(None, "cloud")

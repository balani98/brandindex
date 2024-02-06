# import boto3
import json
import pandas as pd
import io
from BIConnector import *


def aggregate_brands(df):
    # df["date"] = min(df["date"])
    df["date"] = min(df["date"])
    df = (
        df.groupby(
            [
                "brand_name",
                "sector_name",
                "region",
                "date",
                "Report Name",
                "sector_id",
                "segment",
                "moving_average",
                "metric",
            ]
        )
        .agg(
            {
                "volume": "mean",
                "score": "mean",
                "positives": "mean",
                "negatives": "mean",
                "neutrals": "mean",
                "positives_neutrals": "mean",
                "negatives_neutrals": "mean",
            }
        )
        .reset_index()
    )
    return df


def aggregate_weekly(df):
    df["date"] = min(df["date"])

    df = (
        df.groupby(
            [
                "date",
                "Report Name",
                "sector_id",
                "segment",
                "moving_average",
                "metric",
                "region",
                "brand_name",
                "sector_name",
            ]
        )
        .agg(
            {
                "volume": "mean",
                "score": "mean",
                "positives": "mean",
                "negatives": "mean",
                "neutrals": "mean",
                "positives_neutrals": "mean",
                "negatives_neutrals": "mean",
            }
        )
        .reset_index()
    )
    return df


def enrich_data_frame(df, query_index, df_all_sectors, df_sector_brands, has_dma):
    df.fillna(0, inplace=True)
    df = df.drop(columns=["custom_sector_uuid"])
    df = df.replace({"query_index": query_index})
    df = df.rename(
        columns={
            "query_id": "segment",
            "query_index": "moving_average",
            "analysis_id": "Report Name",
        }
    )
    df["moving_average"] = df["moving_average"].div(7)

    df_all_sectors = df_all_sectors.rename(columns={"label": "sector_name"})
    df_sector_brands = df_sector_brands.rename(columns={"label": "brand_name"})
    df = pd.merge(
        df,
        df_all_sectors[["id", "sector_name"]],
        how="left",
        left_on=["sector_id"],
        right_on=["id"],
    ).drop(columns=["id"])
    df = pd.merge(
        df,
        df_sector_brands[["id", "brand_name"]],
        how="left",
        left_on=["brand_id"],
        right_on=["id"],
    ).drop(columns=["id"])
    df["brand_name"] = df["brand_name"].str.replace("Floor & Décor", "Floor & Decor")
    return df


def sentiment_percentage_cols(df):
    df["positive_yes"] = df["positives"] * df["volume"] / 100
    df["negative_no"] = df["negatives"] * df["volume"] / 100
    return df


def push_to_s3(buffer, s3bucket, brand_name, start_date, end_date):
    s3 = boto3.client("s3")
    s3.put_object(
        Body=buffer.getvalue(),
        Bucket=s3bucket,
        Key="BI_"
        + brand_name
        + "/{brand}_{start_date}_{end_date}.csv".format(
            brand=brand_name, start_date=start_date, end_date=end_date
        ),
    )


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def output_status_message(message):
    print(message)


####################################3
def usbank_dfs(path1, json_filename, session, start_date, end_date):
    query_index = {}
    sectors_and_regions = []
    with open(path1 + json_filename, "r") as f:
        content = f.read()
        content = content.replace(
            "###start_date###", start_date.strftime("%Y-%m-%d")
        ).replace("###end_date###", end_date.strftime("%Y-%m-%d"))

    data = json.loads(content)
    # index = 0

    response = run_analysis(session, data)
    temp_frame = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
    return temp_frame


def usbank_df_process(df):
    df = df.rename(
        columns={
            "query_id": "Demo",
            "sector_id": "Sector Code",
            "brand_id": "brand",
            "positives": "positive_yes",
            "negatives": "negative_no",
            "query_index": "Moving Average",
            "analysis_id": "Report Name",
            "custom_sector_uuid": "Sector Name",
        },
    )
    df["Moving Average"] = 14

    df["date"] = pd.to_datetime(df["date"])
    df["positive_yes"] = df["positive_yes"] * df["volume"] / 100
    df["negative_no"] = df["negative_no"] * df["volume"] / 100
    df["neutrals"] = df["neutrals"] * df["volume"] / 100
    df["unaware"] = df["volume"] - (
        df["positive_yes"] + df["negative_no"] + df["neutrals"]
    )
    df["unaware"] = df["unaware"].mask(df["unaware"] < 1, 0)
    df = df.drop(
        columns=[
            "region",
            "positives_neutrals",
            "negatives_neutrals",
        ]
    )

    df = df.rename(
        columns={
            "neutrals": "neutral",
        },
    )

    df.fillna(0, inplace=True)

    USB_COMP = "fba93d07-effb-4c05-9537-7fcbbf94efc4"
    USB_COMP_PEER = "acd7506f-04e5-4e36-9537-a04d4b5ef796"

    df["Sector Name"].replace(
        {USB_COMP: "USB Comp", USB_COMP_PEER: "USB Peer Comp Set"}, inplace=True
    )

    df.loc[df["Sector Name"] == "USB Comp", ["Sector Code", "brand"]] = [
        -110,
        "USB Comp",
    ]
    df.loc[df["Sector Name"] == "USB Peer Comp Set", ["Sector Code", "brand"]] = [
        -126,
        "USB Peer Comp Set",
    ]

    df["Sector Name"].replace({0: "Consumer Banks"}, inplace=True)

    df["brand"].replace(
        {
            12001: "Bank of America",
            12005: "Chase",
            12004: "BB & T",
            12009: "Fifth-Third",
            12006: "Citibank",
            12013: "KeyBank",
            12017: "PNC Bank",
            12018: "Regions Bank",
            12019: "SunTrust",
            12021: "US Bank",
            12024: "Wells Fargo",
            1001320: "TCF Bank",
        },
        inplace=True,
    )

    ## aggregate_data

    df["volume"] = pd.to_numeric(df["volume"])
    df["score"] = pd.to_numeric(df["score"])
    df["positive_yes"] = pd.to_numeric(df["positive_yes"])
    df["negative_no"] = pd.to_numeric(df["negative_no"])
    df["neutral"] = pd.to_numeric(df["neutral"])
    df["unaware"] = pd.to_numeric(df["unaware"])

    df["date"] = min(df["date"])
    df = (
        df.groupby(
            [
                "date",
                "brand",
                "Demo",
                "metric",
                "Moving Average",
                "Sector Code",
                "Sector Name",
                "Report Name",
            ]
        )
        .agg(
            {
                "volume": "sum",
                "score": "mean",
                "positive_yes": "sum",
                "negative_no": "sum",
                "neutral": "sum",
                "unaware": "sum",
            }
        )
        .reset_index()
    )

    df = df[
        [
            "date",
            "brand",
            "metric",
            "volume",
            "score",
            "positive_yes",
            "negative_no",
            "neutral",
            "unaware",
            "Demo",
            "Moving Average",
            "Sector Code",
            "Sector Name",
            "Report Name",
        ]
    ]
    return df


####################################################################

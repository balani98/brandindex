# import boto3
import pandas as pd
import numpy as np


def calculate_no_moving_avg_scores(df):
    df["date"] = min(df["date"])

    df = (
        df.groupby(
            [
                "date",
                "Report Name",
                "sector_id",
                "brand_id",
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
                "volume": "sum",
                "score": "mean",
                "positives": "sum",
                "positive_yes": "sum",
                "negatives": "sum",
                "negative_no": "sum",
                "neutrals": "mean",
                "positives_neutrals": "mean",
                "negatives_neutrals": "mean",
            }
        )
        .reset_index()
    )

    df["score"] = np.where(
        (
            (df["metric"] == "adaware")
            | (df["metric"] == "aided")
            | (df["metric"] == "wom")
            | (df["metric"] == "consider")
            | (df["metric"] == "likelybuy")
            | (df["metric"] == "former_own")
            | (df["metric"] == "current_own")
        ),
        (df["positive_yes"]) * 100 / df["volume"],
        df["score"],
    )

    df["score"] = np.where(
        (
            (df["metric"] == "impression")
            | (df["metric"] == "satisfaction")
            | (df["metric"] == "quality")
            | (df["metric"] == "recommend")
            | (df["metric"] == "value")
            | (df["metric"] == "buzz")
            | (df["metric"] == "reputation")
        ),
        (df["positive_yes"] - df["negative_no"]) * 100 / df["volume"],
        df["score"],
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
                "brand_id",
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


def enrich_data_frame(
    df,
    query_moving_average,
    df_all_sectors,
    df_sector_brands,
    has_dma,
    moving_average=84,
):
    df.fillna(0, inplace=True)
    df = df.drop(columns=["custom_sector_uuid"])
    df = df.drop(columns=["query_index"])
    df["moving_average"] = moving_average
    for qi in query_moving_average:
        df.loc[(df.query_id == qi), "moving_average"] = query_moving_average[qi]

    df = df.rename(columns={"query_id": "segment", "analysis_id": "Report Name"})
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

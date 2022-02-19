import boto3
import pandas as pd


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

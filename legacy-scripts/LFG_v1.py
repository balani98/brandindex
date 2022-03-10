from brandindex import BI
import pandas as pd
import numpy as np
from datetime import timedelta, date, datetime
from dateutil.relativedelta import *
import time

# 0 0 1 * * For running first day of month at midnight

EMAIL = "YGAPI@xmedia.com"
PASSWORD = "YouGov123"
today = date.today()
END_DATE = (today - timedelta(days=1)).strftime("%Y-%m-%d")  #'2021-11-30'
START_DATE = (today - relativedelta(months=1)).strftime("%Y-%m-%d")  #'2021-11-01'
OUTPUT_NAME = "BrandIndex_LFG_{}".format(
    datetime.strptime(END_DATE, "%Y-%m-%d").strftime("%b%y")
)

S3_BUCKET = "rb.product.finished"
S3_KEY = "BI_LFG/" + OUTPUT_NAME
SECTOR = 27  # Financial Services-Investment Advisors
SECTOR2 = 25  # Insurance
MOVING_AVG = 56  # 8 weeks
MOVING_AVG_16 = 112  # 16 weeks
RESAMPLE_TYPE = "month"
SCORING = "net_score"

""" BRANDS OF INTEREST """

# Sector 27
brand_list = [
    "Fidelity",
    "Vanguard",
    "Nationwide",
    "Voya Financial",
    "Prudential",
    "TIAA",
    "Lincoln Financial",
    "Brighthouse Financial",
    "T. Rowe Price",
    "Empower Retirement",
]
# Sector 25
brand_list2 = [
    "Guardian",
    "AIG",
    "The Hartford",
    "John Hancock",
    "MetLife",
    "New York Life",
    "Northwestern Mutual",
    "Pacific Life",
    "Transamerica",
    "Nationwide",
    "Prudential",
    "Lincoln Financial",
    "Mass Mutual",
    "Aflac",
    "Cigna",
    "Athene",
]

""" CUSTOM SECTORS """

# LFG Insurance Competitor Sector
LFG_COMP = "b553cd97-f3c5-42ac-939b-adb8b96b7a64"
# LFG FA Competitor Sector
LFG_FA_COMP = "c29ad5cc-2240-47dc-b228-9840bd2fc9dd"
LFG_TIER1 = "d969d532-2655-4df6-a610-c57d65b383b8"
LFG_TIER2 = "e2c2e5f9-b286-4420-8950-a4b7c88ee458"
LFG_TIER3 = "a862fdfc-92aa-4d79-8eea-b1a7916fad7c"

""" FILTERS """

FILTERS = [
    ["bixdemo_gender", [1, 2], "Total Population"],
    [
        "bixdemo_agegranular",
        [
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
        ],
        "A22-44",
    ],
    ["bixdemo_ageall", [4, 5], "A45-64"],
    ["bixdemo_ageall", [5, 6], "A55+"],
    ["bixdemo_ageall", [2, 3, 4], "A25-54"],  # 4
    ["bixdemo_income2", [8, 9, 10, 11, 12, 13, 14], "HHI: $50k"],
    ["bixdemo_ageall", [4, 5, 6], "A45"],
    ["bixdemo_income2", [12, 13, 14], "HHI: +$100k"],
    [
        "a861bac4-2bf2-46c8-bd41-d6e91bf4cabe",
        ["56d4b78a-5c9c-4658-bd6b-5be32e04351e"],
        "Security Seekers",
    ],
    [
        "b0f45532-9035-45ae-8332-7ccd0b07fb53",
        ["4309c0b7-a380-40a8-a945-173ee9a9b371"],
        "Old Aspirers",
    ],
    [
        "c149d3bb-3317-472d-acd4-53be2aed3980",
        ["860ac0b6-0dfa-44ff-9a9a-ea17b62865f8"],
        "Younger Aspirers",
    ],
    [
        "inputzipdma_combined2",
        [504, 508, 524, 528, 533, 669, 753, 807],
        "Radio Markets",
    ],
    [
        "inputzipdma_combined2",
        [504, 508, 515, 528, 533, 613, 635, 753, 807, 825],
        "TV Markets",
    ],
    [
        "d5d652f6-64ac-474b-996e-bd580f967d17",
        ["bf13f29f-bcec-4aa0-8826-a2c10745e1f7"],
        "Job: Investment or FA",
    ],
]

bi = BI(EMAIL, PASSWORD)
bi.login()

# Get all brands within each sector
brands = bi.get_brands(SECTOR)
brands2 = bi.get_brands(SECTOR2)

# Only get the brands we are interestd in
brandInfo = []
for brand in brands.values():
    if brand["label"] in brand_list:
        brandInfo.append(brand)

for brand in brands2.values():
    if brand["label"] in brand_list2:
        brandInfo.append(brand)

# Add custom sectors
brandInfo.append(LFG_COMP)
brandInfo.append(LFG_FA_COMP)
brandInfo.append(LFG_TIER1)
brandInfo.append(LFG_TIER2)
brandInfo.append(LFG_TIER3)

# Get all sectors
sectors = bi.get_sectors()

query = [[]]

count = 1

for brand in brandInfo:
    # **** CUSTOM SECTORS ****
    if type(brand) == str:
        query.append([])

        query[-1] += bi.build_query_grouped(
            brand, FILTERS[0:4], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_query_grouped(
            brand, [[FILTERS[4]], [FILTERS[5]]], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_query_grouped(
            brand, [[FILTERS[6]], [FILTERS[7]]], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_query_grouped(
            brand, FILTERS[8:13], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_query_grouped(
            brand, [FILTERS[13]], MOVING_AVG_16, START_DATE, END_DATE
        )
    # **** BRANDS ****
    else:  # type == Dict
        # @TODO: why?????
        if count % 9 == 0:
            query.append([])

        # @TODO: why grouped this way -> guessing wanting to see these splits??

        query[-1] += bi.build_queries_individual(
            brand, FILTERS[0:4], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_queries_individual(
            brand, [[FILTERS[4]], [FILTERS[5]]], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_queries_individual(
            brand, [[FILTERS[6]], [FILTERS[7]]], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_queries_individual(
            brand, FILTERS[8:13], MOVING_AVG, START_DATE, END_DATE
        )
        query[-1] += bi.build_queries_individual(
            brand, [FILTERS[13]], MOVING_AVG_16, START_DATE, END_DATE
        )

        count += 1

df1 = bi.execute_analyses(query[0], "LFG")

queryCount = 1

while queryCount < len(query):
    try:
        print("Current Query Count: {}".format(queryCount))
        df1 = pd.concat([df1, bi.execute_analyses(query[queryCount], "LFG")])
        queryCount += 1
    except:
        print("Error-Sleeping")
        time.sleep(60)

df = df1

df = bi.clean_dataframe(df, dict(brands, **brands2), sectors)

df["custom_sector_uuid"].replace(
    {
        LFG_COMP: "LFG Insurance Competitor",
        LFG_FA_COMP: "LFG FA Competitor Sector",
        LFG_TIER1: "LFG-Tier 1",
        LFG_TIER2: "LFG-Tier 2",
        LFG_TIER3: "LFG-Tier 3",
    },
    inplace=True,
)

df[["volume", "score", "positive_yes", "negative_no", "neutrals", "unaware"]] = df[
    ["volume", "score", "positive_yes", "negative_no", "neutrals", "unaware"]
].apply(pd.to_numeric)

df = bi.aggregate_data(df, "MS")

# Formatting to match old v0 file for datorama ingestion
df.rename(columns={"sector_code": "Sector Name", "neutrals": "neutral"}, inplace=True)
df["Report Name"] = "LFG"

df.loc[:, "Sector Code"] = df["Sector Name"]
df["Sector Code"].replace(
    {sectors[sect_id]["label"]: int(sect_id) for sect_id in sectors.keys()},
    inplace=True,
)

df["Sector Name"] = df["custom_sector_uuid"] + df["Sector Name"]
df.drop(columns=["custom_sector_uuid"], inplace=True)

df.loc[df["Sector Name"] == "LFG Insurance Competitor", "Sector Code"] = -175
df.loc[df["Sector Name"] == "LFG FA Competitor Sector", "Sector Code"] = -176
df.loc[df["Sector Name"] == "LFG-Tier 3", "Sector Code"] = -179
df.loc[df["Sector Name"] == "LFG-Tier 2", "Sector Code"] = -178
df.loc[df["Sector Name"] == "LFG-Tier 1", "Sector Code"] = -177

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

# @CALLOUT -> work around for now since moving average is hardcoded
# to whatever the first value seen is
df["Moving Average"] = np.where(
    df["Demo"] == "Job: Investment or FA", 16.0, df["Moving Average"]
)

# Fix old aspirers name
df["Demo"] = np.where(df["Demo"] == "Old Aspirers", "Older Aspirers", df["Demo"])
bi.send_to_s3(df, S3_BUCKET, S3_KEY)

print("File Uploaded Successfully!")

from brandindex import BI
import pandas as pd
from datetime import timedelta, datetime, date

# Sunday TO Saturday

EMAIL = "YGAPI@xmedia.com"
PASSWORD = "YouGov123"
SECTOR = 12
S3_BUCKET = "rb.product.finished"
MOVING_AVG = 98
RESAMPLE_TYPE = "week"
SCORING = "net_score"
START_DATE = 8
END_DATE = 1
# START_DATE = "2021-06-29"
# END_DATE = "2021-07-04"
OUTPUT_NAME = "BrandIndex__US__Bank__"
S3_KEY = "BI_US_BANK_Weeklydata/" + OUTPUT_NAME

BRANDS = [
    "Bank of America",
    "Chase",
    "BB & T",
    "Fifth-Third",
    "Citibank",
    "KeyBank",
    "PNC Bank",
    "Regions Bank",
    "SunTrust",
    "US Bank",
    "Wells Fargo",
    "TCF Bank",
]

USB_COMP = "fba93d07-effb-4c05-9537-7fcbbf94efc4"
USB_COMP_PEER = "acd7506f-04e5-4e36-9537-a04d4b5ef796"

FILTERS = [
    [
        "inputstate",
        [
            4,
            5,
            6,
            8,
            13,
            16,
            17,
            18,
            19,
            20,
            21,
            26,
            27,
            29,
            30,
            31,
            32,
            35,
            38,
            39,
            40,
            41,
            42,
            46,
            47,
            48,
            49,
            51,
            53,
            54,
            55,
            56,
        ],
        "Total Population",
    ],
    ["bixdemo_race2", [2], "AFAM"],
    ["bixdemo_race2", [4], "Asian"],
    ["bixdemo_race2", [3], "Hispanic"],
    ["bixdemo_closegay4", [1], "LGBTQ+"],
    ["bixdemo_income2", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "HHI<80K"],
    ["bixdemo_income2", [11, 12, 13, 14], "HHI>80K"],
    ["bixdemo_ageall", [1, 2], "18-34"],
    ["bixdemo_ageall", [3, 4], "35-54"],
    ["bixdemo_ageall", [5], "55+"],
    ["bixdemo_ageall", [2, 3, 4], "25-54"],  # 10
    ["bank_account_future", [1, 2, 4], "Deposits"],
    ["bank_account_future", [3, 12], "SMB"],
    ["bank_account_future", [5], "Home mortgage"],
    ["bank_account_future", [6], "Home equity loans"],
    ["bank_account_future", [7], "Retirement"],
    ["bank_account_future", [8], "Investment"],
    ["bank_account_future", [9], "RPS/Credit"],
    ["bank_account_future", [10], "Student"],
    ["bank_account_future", [13], "Auto"],
    ["bixdemo_gender", [2], "Female"],
    ["customer_status", [0], "Current Customer"],  # 21
    [
        "inputstate",
        [
            4,
            5,
            6,
            8,
            16,
            17,
            18,
            19,
            20,
            21,
            27,
            29,
            30,
            31,
            32,
            35,
            38,
            39,
            41,
            46,
            47,
            49,
            53,
            55,
            56,
        ],
        "Footprint",
    ],
    ["top60dmas2_inputzip", [3], "Chicago"],
    ["top60dmas2_inputzip", [13], "Seattle"],
    ["top60dmas2_inputzip", [15], "Minneapolis"],
    ["top60dmas2_inputzip", [17], "Denver"],
    ["top60dmas2_inputzip", [34], "Milwaukee"],
    ["top60dmas2_inputzip", [35], "Cincinnati"],
]

bi = BI(EMAIL, PASSWORD)
bi.login()

brands = bi.get_brands(SECTOR)

brandInfo = []
for brand in brands.values():
    if brand["label"] in BRANDS:
        brandInfo.append(brand)

brandInfo.append(USB_COMP)
brandInfo.append(USB_COMP_PEER)

sectors = bi.get_sectors()

query = [[], [], []]
query2 = []
count = 0
for brand in brandInfo:
    if type(brand) == str:
        query[count // 8] += bi.build_query_grouped(
            brand, [FILTERS[0]], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_query_grouped(
            brand, [FILTERS[1:21], [FILTERS[0]]], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_query_grouped(
            brand, [FILTERS[22]], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_query_grouped(
            brand, FILTERS[23:], MOVING_AVG, START_DATE, END_DATE
        )
        count += 8
    else:
        query[count // 8] += bi.build_queries_individual(
            brand, [FILTERS[0]], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_queries_individual(
            brand, [FILTERS[1:21], [FILTERS[0]]], MOVING_AVG, START_DATE, END_DATE
        )
        customer_filter = [
            "brand.{}.{}".format(brand["id"], FILTERS[21][0]),
            FILTERS[21][1],
            FILTERS[21][2],
        ]
        query[count // 8] += bi.build_queries_individual(
            brand, [customer_filter], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_queries_individual(
            brand, [FILTERS[22]], MOVING_AVG, START_DATE, END_DATE
        )
        query[count // 8] += bi.build_queries_individual(
            brand, FILTERS[23:], MOVING_AVG, START_DATE, END_DATE
        )
        count += 1


df = bi.execute_analyses(query[0], "USB_Weekly_Rollup")
df2 = bi.execute_analyses(query[1], "USB_Weekly_Rollup")
df3 = bi.execute_analyses(query[2], "USB_Weekly_Rollup")

df = pd.concat([df, df2, df3])

df = bi.clean_dataframe(df, brands, sectors)
df["custom_sector_uuid"].replace(
    {USB_COMP: "USB Comp", USB_COMP_PEER: "USB Peer Comp Set"}, inplace=True
)

df = bi.aggregate_data(df, "W-Sun")
df["Demo"] = df["Demo"].str.split("+", n=1, expand=True)[0]

# Formatting to match v0 for datorama
df.rename(columns={"sector_code": "Sector Name", "neutrals": "neutral"}, inplace=True)
df["Report Name"] = "USB-Weekly"

df.loc[:, "Sector Code"] = df["Sector Name"]
df["Sector Code"].replace(
    {sectors[sect_id]["label"]: int(sect_id) for sect_id in sectors.keys()},
    inplace=True,
)

df["Sector Name"] = df["custom_sector_uuid"] + df["Sector Name"]
df.drop(columns=["custom_sector_uuid"], inplace=True)

df.loc[df["Sector Name"] == "USB Comp", ["Sector Code", "brand"]] = [-110, "USB Comp"]
df.loc[df["Sector Name"] == "USB Peer Comp Set", ["Sector Code", "brand"]] = [
    -126,
    "USB Peer Comp Set",
]

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
# bi.send_to_s3(df, S3_BUCKET, S3_KEY)
# df.to_csv(f"~/Desktop/{OUTPUT_NAME}{START_DATE}_{END_DATE}.csv")
print("Success!")

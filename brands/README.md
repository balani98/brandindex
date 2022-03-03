
# How to modify Brand's input for BrandIndex API

Each brand has it's own python script inside **/brands** folder, and when ever there is a change in particular brands input, just go to the **{brand}-generatejson.py** file and change required details and run the script, this will produce a new **{brand}.json** file in current directory and then upload the newly generate json file to **s3://rb.product.finished/lambda_config/**


## Examples

### Add a new competitor to Brands input

LLFlooring example, go to **LLFlooring-generatejson.py** script and add a new brand to the brands dictionary and run the script and upload the newly generate json file to s3 bucket (**s3://rb.product.finished/lambda_config/**)

```
brands = [
    {"brand_id": 34008, "region": "us", "sector_id": 34},
    {"brand_id": 1003669, "region": "us", "sector_id": 34},
    {"brand_id": 34014, "region": "us", "sector_id": 34},
]
```

### Change moving average 

LLFlooring example, go to **LLFlooring-generatejson.py** script and change moving_average variable to required moving average (in days) and run the script and upload the newly generate json file to s3 bucket (**s3://rb.product.finished/lambda_config/**)

```
moving_average = 56
```

### Add / Remove DMA

Torrid example, go to **Torrid-generatejson.py** script and add/remove DMA items in DMAs list and then run the script and upload the newly generate json file to s3 bucket (**s3://rb.product.finished/lambda_config/**)

```
DMAs = {
    "Los Angeles CA": "2",
    "Chicago IL": "3",
    "Dallas-Ft. Worth TX": "5",
    "Cleveland-Akron (Canton) OH": "19",
    "Indianapolis IN": "26",
    "San Diego CA": "28",
    "Sacramento-Stockton-Modesto CA": "20",
    "New York, NY": "1",
    "Houston TX": "10",
    "Atlanta GA": "9",
    "Columbus OH": "32",
    "Philadelphia PA": "4",
    "Seattle-Tacoma WA": "13",
    "Detroit MI": "11",
    "Phoenix AZ": "12",
    "Boston MA-Manchester NH": "7",
    "Austin TX": "40",
    "Minneapolis-St. Paul MN": "15",
    "San Antonio, TX": "36",
}
```

### Add/Remove/Modify filter

Torrid example, go to **Torrid-generatejson.py** script and change filters array (to add/remove/update) and run the script and upload the newly generate json file to s3 bucket (**s3://rb.product.finished/lambda_config/**)


```
filters = [
    {
        "segment": "F 18-24",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {"expression": "bixdemo_agegranular in [1,2,3,4,5,6,7]"},
        ],
    },
    {
        "segment": "F 18-44",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {
                "expression": "bixdemo_agegranular in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]"
            },
        ],
    },
    {
        "segment": "F 25-44",
        "filters": [
            {"expression": "bixdemo_gender in [2]"},
            {
                "expression": "bixdemo_agegranular in [8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]"
            },
        ],
    },
    {"segment": "NAT REP (No filter)", "filters": []},
]

```

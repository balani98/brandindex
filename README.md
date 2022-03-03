![logo](https://camo.githubusercontent.com/205f6f35396a351dffbc19f001e60adcec3065c6e8a527134d86f969a3163a21/68747470733a2f2f64323564323530367366623934732e636c6f756466726f6e742e6e65742f722f37362f5947562d4272616e64496e6465782e706e67)

## Folder structure

- /**api** - contains all the code that downloads data from BI API
- /**brands** - contains each brands generatejson.py file, which contains all the details about filters, competitors and DMAs etc.
- /**legacy-scripts** - contains old BI scripts of each brands which is moved to lambda scheduling

## Client Overview

| client | cadence | time | active | code |
| -- | -- | -- | -- | -- |
| torrid | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Torrid-generatejson.py) |
| ll flooring | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/LLFlooring-generatejson.py) |


## AWS Lambda deployment

Techical details
- Lambda function   : **XM_brand_index_API_data_pull**
- Language          : **Python 3.8**
- Scheduled         : **Every Monday 4 AM EST**
- S3 bucket         : **rb.product.finished**
- API               : https://api.brandindex.com/v1/ 


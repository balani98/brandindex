# xm-brandindex-v1
This is the repo for having python wrapper to download BrandIndex API data.

## Folder structure

- /**api** - contains all the code that downloads data from BI API
- /**brands** - contains each brands generatejson.py file, which contains all the details about filters, competitors and DMAs etc.
- /**legacy-scripts** - contains old BI scripts of each brands which is moved to lambda scheduling

# Client Overview

| client | cadence | time | active | code |
| -- | -- | -- | -- | -- |
| torrid | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Torrid-generatejson.py) |
| ll flooring | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/LLFlooring-generatejson.py) |

# BrandIndex API - V1 
## AWS Lambda deployment

Techical details
- Lambda function   : **XM_brand_index_API_data_pull**
- Language          : **Python 3.8**
- Scheduled         : **Every Monday 4 AM EST**
- S3 bucket         : **rb.product.finished**
- API               : https://api.brandindex.com/v1/ 


![logo](https://api.brandindex.com/v1/static/img/logo.png)

### YouGov BrandIndex API
For detailed documentation : [Refer Here](./BrandIndex.md)
## Folder structure

- /**api** - contains all the code that downloads data from BI API
- /**brands** - contains each brands generatejson.py file, which contains all the details about filters, competitors and DMAs etc.
- /**legacy-scripts** - contains old BI scripts of each brands which is moved to lambda scheduling

## Client Overview

| client | cadence | time | active | code |
| -- | -- | -- | -- | -- |
| torrid | xxx | xxx | no | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Torrid-generatejson.py) |
| ll flooring | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/LLFlooring-generatejson.py) |
| Empower | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Empower_generatejson.py) |
| Ihealth | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Empower_generatejson.py) |
| USBank_Health | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/USBHealth-generatejson.py) |
| PlanetFitness | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/tree/main/brands/Planetfitness) |
| PlanetFitness_DMA | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/tree/main/brands/Planetfitness-DMA) |
| LFG | xxx | xxx | yes | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/tree/main/brands/LFG) |
| EWC | xxx | xxx | no | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/EWC-generatejson.py) |
| Tommy | xxx | xxx | no | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/tree/main/brands/Tommy) |
| Invesco | xxx | xxx | no | [link](https://github.com/CrossmediaHQ/xm-brandindex-v1/blob/main/brands/Invesco-generatejson.py) |

## AWS Lambda and EC2 deployment 

Techical details
### Invesco and Torrid
- Lambda function   : **XM_brand_index_API_data_pull**
- Language          : **Python 3.8**
- Brands            : **Invesco and Torrid**
- Scheduled         : **Every Monday 4 AM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory - Invesco    : **s3://rb.product.finished/BI_Invesco/**
- S3 directory - Torrid    : **s3://rb.product.finished/BI_Torrid/**
- API               : https://api.brandindex.com/v1/

### IHealth
- Lambda function : **XM_brand_index_API_data_pull_IHealth**
- Language          : **Python 3.8**
- Brands            : **IHealth**
- Scheduled         : **Every Monday 5:55 PM  EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_IHealth/**
- API               : https://api.brandindex.com/v1/

### USBankHealth
- Lambda function : **XM_brand_index_API_data_pull_USBankHealth**
- Language          : **Python 3.8**
- Brands            : **USBank**
- Scheduled         : **1st day of every month 7:10 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_USBankHealth/**
- API               : https://api.brandindex.com/v1/
- Detailed Documentation : [Refer Here](./api/USB_Health/README.md)


### LFG2
- Lambda function : **BrandIndex_LFG2_monthly**
- Language          : **Python 3.8**
- Brands            : **LFG**
- Scheduled         : **2nd day of every month 6:15 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_LFG2/**
- API               : https://api.brandindex.com/v1/

### Planetfitness
- Lambda function : **XM_brand_index_API_data_pull_Planetfitness**
- Language          : **Python 3.8**
- Brands            : **Planetfitness**
- Scheduled         : **1st day of every month 8:20 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_PlanetFitness/**
- API               : https://api.brandindex.com/v1/
- Detailed Documentation : [Refer Here](./api/Planetfitness/README.md)


### Planetfitness_DMA
- EC2 : **i-0107e1a7411cf324c (BrandIndex_monthly_Planetfitness2)**
- Language          : **Python 3.10.12**
- Brands            : **Planetfitness**
- Scheduled         : **2nd day of every month 2:03 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_PlanetFitness_DMA/**
- API               : https://api.brandindex.com/v1/

### Empower
- Lambda function : **XM_brand_index_API_data_pull_Monthly**
- Language          : **Python 3.8**
- Brands            : **Empower**
- Scheduled         : **1st day of every month 6:15 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_Empower/**
- API               : https://api.brandindex.com/v1/
- Detailed Documentation : [Refer Here](./api/Empower/README.md)

### LL Flooring
- Lambda function : **XM_brand_index_API_data_pull_LLF**
- Language          : **Python 3.8**
- Brands            : **LLF**
- Scheduled         : **Every Wednesday 5:40 PM EST**
- S3 bucket         : **rb.product.finished**
- S3 directory      : **s3://rb.product.finished/BI_Empower/**
- API               : https://api.brandindex.com/v1/
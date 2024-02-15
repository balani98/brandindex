### USBAnk Health API connector description
USBank health API connector is little different from other brands. Here along with metrics extraction , some aggregations have to be done as well. 

There are 2 set of Brands : 
* **Large competitive set :**  Wells Fargo , Chase , Citibank , Bank Of America , Capital One Bank , PNC Bank , US Bank
* **Small competitive set :** Fifth-Third, TCF Bank, Truist, Regions Bank

Aggregation needs to be performed for these groups (mean of all the scores), and the aggregated data is joined with the original data.
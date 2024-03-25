### LFG API connector description
The LFG API connector distinguishes itself from other brands by incorporating additional transformations beyond data extraction from the API. These transformations include:
#### Tier Groupings
A additional column is added to represent tier groupings for Brands . Tier groupings are calculated based on Total Population **Aided brand awareness** Score. Business usecase of is to divide the brands in their Tier groupings .
The breakout of tier groupings is done as follows:
* **Tier 1 :** Total Population Awareness Score > 60% .
* **Tier 2 :** Total Population Awareness Score > 30% and Total Population Awareness Score < 60% .
* **Tier 3 :** Total Population Awareness < 30% .
    
### Example
>  If Aflac falls under 'Tier 1' as per its logic  . Now logic will reassign all the other metrics (adaware , wom and statisfaction etc )   under Aflac as 'Tier 1' . Same will be replicated across all metrics and audiences /all demographic groups for the Aflac . 

#### Category
 It represent the average of  all the brands This represents the average of all the brands. This calculation is performed for all the metrics present. The business use case is to obtain the competitive aggregate value for **Total Population, Affluent Aspirers 30 - 49, and Affluent Seekers 50 - 69** audiences across all metrics.



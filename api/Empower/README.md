### Empower API connector description
Empower API connector is little different from other brands. Here data has to be pulled for both moving and no moving averages. This data has to be aggregated (mean of all the scores) as well . Moving average scores can be calculated by easily averaging but No Moving averages scores have different formulation they can't be calculated just be averaging .

####  No moving average scores formulation : 
* For the metrics ( Ad awareness , Aided awarenss , Word of mouth exposure , consideration , Purchase Intent , Current Customer , Former Customer) :  **(Sum(Positives_yes))/Volume**

* For the metrics (Buzz, Impression, Satisfaction, Quality, Corporate repuatation, value, Recommendation) : **(Sum(Positives_yes) - Sum(negatives_no))/Volume**

* Index can be calculated by averaging the scores for Impression, Satisfaction, Quality, Corporate Reputation, Value, and Recommendation


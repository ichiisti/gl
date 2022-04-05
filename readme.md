Gained and Lost Clients

Objectives:
          1. calculation / aggregation of yearly quantity for gained and lost customers.
          2. tracking churn rate for B2C customers.
          3. highlight net/gained/lost quantity for B2B customers.
          4. tracking main competitors for B2C and B2B customers.

Calculation frequency: Calculation is done on monthly basis for B2C ( business to consumer) and B2B ( business to business) consumers.
Data source: excel files

Data processing:
=> import: data sources are uploaded to database using the ETL script
=> database : in the database the informations are organized on 3 levels ( level 1 - interface tables / raw data, level 2 - calculation / aggregate data , level 3 - data set table for analysis)
=> data processing : for B2C costumers yearly quantities are calculated taking into account the county and historical data, for B2B clients the volumes are calculated taking into account historical data. Data processing is done using the T-SQL procedures. During data processing messages are inserted into log table.





 

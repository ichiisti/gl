# **Gained and Lost Clients**

*Objectives:*

1. Calculation / aggregation of yearly quantity for gained and lost customers.
2. Tracking churn rate for B2C customers.
3. Highlight net/gained/lost quantity for B2B customers.
4. Tracking main competitors for B2C and B2B customers.

*Calculation frequency:* 
Calculation is done on monthly basis for B2C ( business to consumer) and B2B ( business to business) consumers.

*Data source:* 
excel files

*Data processing:*
1. Import: data sources are uploaded to database using the ETL script.
2. Database : in the database the informations are organized on 3 levels\
          1. level 1 - interface tables / raw data\
          2. level 2 - calculation / aggregate data\
          3. level 3 - data set table for analysis
5. Data processing : for B2C costumers yearly quantities are calculated taking into account the county and historical data, for B2B clients the volumes are calculated taking into account historical data. Data processing is done using the T-SQL procedures. During data processing messages are inserted into log table.





 

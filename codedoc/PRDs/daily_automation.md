Context
-------
Integration of the daily processed data into the automated processing pipeline.

Goal
----
- provide appropriate datasets, that are preprocessed in different steps
- the datasets must be updated everyday with the daily data, meaning it has to contain the quarterly and the daily data

Traps
-----
- As soon as quarterly data is available, the daily data for that quarter is being removed.
  This means, we need a way to also remove the according data from the automated datasets,
  or we need to detect which quarters/data are still just available from daily data.
  So, maybe a good idea is to have a first step, that check and clears the automated daily data if necessary.
- we wanna do this as optmized as possible. however, once the data is filtered, the pure daily data should rather have 
  a limited size.
- there can be one or two quarters that are only available from daily data, since the quarterly data is usually available about two weeks after the quarter started.
  
  

General Flow
------------
- create the data for the quarterly files as it is already done within the 
  file src/secfsdstools/x_examples/automation/memory_optimized_automation.py
- prepare the same sets of data for the daily data
- merge the two datasets together


Daily Data Automation Processing steps
--------------------------------------
1. check and clear daily data if necessary. do this by quarter
  1. filtered and joined by date
  2. standardized by date
  3. all -> wahrscheinlich alles neu bei Änderung? 
2. generate the filtered daily datasets per day and per statement.
3. standardize the daily data per statement
4. 
5. create the "_3_all/_1_joined_by_stmt" dataset by concatenating the "daily data only" and the quarterly data.
6. create the "_3_all/_2_joined" dataset by concatenating all the data from step 4.
7. create the "_3_all/_3_standardized_by_stmt" dataset by concatenating the standardized "daily data only" and the quarterly data. 


Created Code
------------




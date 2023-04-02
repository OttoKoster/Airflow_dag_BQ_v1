# Airflow_DAG_BQ

Description:
It is necessary to write a DAG that will take data from raw Google Big Query tables every day at 3 a.m. server time, combine them and overwrite the data in the resulting table for 30 days.

Tables:

![image](https://user-images.githubusercontent.com/128299550/229361525-07c4c15f-256e-4d1b-9cf9-3e8d6a7a699b.png)

Requirements:
1. It is necessary to combine three sources in the resulting table mart_data.marketing_account using SQL in a Big Query, so that it can be seen on which day, from which source, for which accounts, how much we spend and how many leads on these accounts there are in CRM as a result
2. The final table, where the combined data will need to be written, should overwrite the data only for the last 30 days, and not completely (that is, you cannot use CREATE OR REPLACE TABLE)
3. DAG should work every day at 3 a.m. server time
4. In the parameters of the DAG, it is necessary to specify a condition under which it will terminate with an error if it works for more than 1 hour

Expected result:
A DAG in Airflow consisting of tasks for forming the mart_data.marketing_account table, which stores information about the performance of the advertising source

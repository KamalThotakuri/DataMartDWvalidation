--use onetimeload;
drop table if exists INCREMENTAL_PSA_CUST_SUPP_text;
CREATE TABLE IF NOT EXISTS INCREMENTAL_PSA_CUST_SUPP_text(SEQUENCE_NUMBER DECIMAL(10, 0), UPD_DT DATE, asofdt DATE, ps_cust_ref_id DECIMAL(12, 0), scram_acct_no STRING, account_type STRING, last_4_ssn DECIMAL(4, 0), firstname STRING, middleinitial STRING, lastname STRING, address1 STRING, address2 STRING, address3 STRING, address4 STRING, city STRING, state STRING, zipcode STRING, product_id DECIMAL(3, 0), current_rate DECIMAL(10, 6), account_open_date DATE, funded_current_balance DECIMAL(15, 0), account_status STRING, fraud_code STRING, close_reason_code STRING, response_channel STRING, application_date DATE, app_id STRING, funddate DATE, account_close_date DATE, account_close_reason DATE, date_of_birth DATE, last_deposit_date DATE, last_withdrawal_date DATE, total_num_of_deposits DECIMAL(6, 0), total_num_of_withdrawals DECIMAL(6, 0), wk1amt DECIMAL(15, 2), wk2amt DECIMAL(15, 2), wk3amt DECIMAL(15, 2), wk4amt DECIMAL(15, 2), wk5amt DECIMAL(15, 2), wk6amt DECIMAL(15, 2), wk7amt DECIMAL(15, 2), wk8amt DECIMAL(15, 2), wk9amt DECIMAL(15, 2), wk10amt DECIMAL(15, 2), wk11amt DECIMAL(15, 2), wk12amt DECIMAL(15, 2), wk13amt DECIMAL(15, 2), m1bal DECIMAL(15, 2), m2bal DECIMAL(15, 2), m3bal DECIMAL(15, 2), m4bal DECIMAL(15, 2), m5bal DECIMAL(15, 2), m6bal DECIMAL(15, 2), m7bal DECIMAL(15, 2), m8bal DECIMAL(15, 2), m9bal DECIMAL(15, 2), m10bal DECIMAL(15, 2), m11bal DECIMAL(15, 2), m12bal DECIMAL(15, 2), record_type STRING, household_id STRING, PIN DECIMAL(13, 0), abilitec_consumer_address_link STRING, CMC_DM DATE, PMC_DM DATE, CMC_EM DATE, PMC_EM DATE, lacaddr_only_link STRING, lachouseholdid STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table INCREMENTAL_PSA_CUST_SUPP_text 
select * from INCREMENTAL_PSA_CUST_SUPP;

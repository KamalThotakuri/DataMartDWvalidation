--use onetimeload;
drop table if exists PRS_PM_DEATHMASTER_text;
CREATE TABLE IF NOT EXISTS PRS_PM_DEATHMASTER_text (PIN DECIMAL(13, 0), list_code STRING, ssn DECIMAL(9,0), last_name STRING, zip_code STRING, dob DATE, src_date_of_death DATE, ADD_DT DATE )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table PRS_PM_DEATHMASTER_text 
select * from PRS_PM_DEATHMASTER;

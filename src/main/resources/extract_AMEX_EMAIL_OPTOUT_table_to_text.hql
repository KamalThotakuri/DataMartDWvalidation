--use onetimeload;
drop table if exists AMEX_EMAIL_OPTOUT_text;
CREATE TABLE IF NOT EXISTS AMEX_EMAIL_OPTOUT_text(LIST_CODE STRING, ADD_DT DATE, UPD_DT DATE, email_id STRING, commun_code DECIMAL(2,0), source STRING, authentication_ind STRING, eff_dt STRING, end_dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table AMEX_EMAIL_OPTOUT_text 
select * from AMEX_EMAIL_OPTOUT;

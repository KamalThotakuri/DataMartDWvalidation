--use onetimeload;
drop table if exists OUTWARD_MEDIA_EMAIL_SUPP_text;
CREATE TABLE IF NOT EXISTS OUTWARD_MEDIA_EMAIL_SUPP_text(vendorpin STRING, email_address STRING, first_name STRING, last_name STRING, address STRING, city STRING, state STRING, zip_code STRING, optin_date DECIMAL(28, 9), optin_url STRING, optin_ip STRING, email_suppression_code DECIMAL(8, 0), ADD_DT DATE, UPD_DT DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table OUTWARD_MEDIA_EMAIL_SUPP_text 
select * from OUTWARD_MEDIA_EMAIL_SUPP;

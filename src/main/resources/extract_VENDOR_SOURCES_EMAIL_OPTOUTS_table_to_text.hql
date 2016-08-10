--use onetimeload;
drop table if exists VENDOR_SOURCES_EMAIL_OPTOUTS_text;
CREATE TABLE IF NOT EXISTS VENDOR_SOURCES_EMAIL_OPTOUTS_text(email_address STRING, optout_type_flag STRING, optout_source STRING, add_dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table VENDOR_SOURCES_EMAIL_OPTOUTS_text 
select * from VENDOR_SOURCES_EMAIL_OPTOUTS;

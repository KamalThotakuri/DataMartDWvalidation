--use onetimeload;
drop table if exists INCREMENTAL_EMAIL_ADDRESS_text;
CREATE TABLE IF NOT EXISTS INCREMENTAL_EMAIL_ADDRESS_text(email_address STRING, email_source STRING, email_use STRING, email_optindate STRING, email_regsource STRING, email_optintime STRING, ibba_flag STRING, file_date STRING, old_record STRING, email_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table INCREMENTAL_EMAIL_ADDRESS_text 
select * from INCREMENTAL_EMAIL_ADDRESS;

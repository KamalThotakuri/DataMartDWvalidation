--use onetimeload;
drop table if exists EMBROS_text;
CREATE TABLE IF NOT EXISTS EMBROS(business_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table EMBROS_text 
select * from EMBROS;

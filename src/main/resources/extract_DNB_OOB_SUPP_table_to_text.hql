--use onetimeload;


drop table if exists DNB_OOB_SUPP_text;
CREATE TABLE IF NOT EXISTS DNB_OOB_SUPP_text (bin DECIMAL(13, 0), list_code STRING  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table DNB_OOB_SUPP_text 
select * from DNB_OOB_SUPP;







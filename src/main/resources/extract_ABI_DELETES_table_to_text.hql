--use onetimeload;
drop table if exists ABI_DELETES_text;
CREATE TABLE IF NOT EXISTS ABI_DELETES_text(abi_number STRING, tran_code STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table ABI_DELETES_text 
select * from ABI_DELETES;

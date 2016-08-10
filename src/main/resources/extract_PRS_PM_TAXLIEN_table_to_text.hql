--use onetimeload;
drop table if exists PRS_PM_TAXLIEN_text;
CREATE TABLE IF NOT EXISTS PRS_PM_TAXLIEN_text(PIN DECIMAL(13, 0), list_code STRING, LARGEST_SRC_DATE DATE, ADD_DT DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table PRS_PM_TAXLIEN_text 
select * from PRS_PM_TAXLIEN;

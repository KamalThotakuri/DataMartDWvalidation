--use onetimeload;
drop table if exists PRS_VERTICALS_text;
CREATE TABLE IF NOT EXISTS PRS_VERTICALS_text(PIN DECIMAL(13, 0), PTYPE STRING, STYPE STRING, ADD_DT DATE, UPD_DT DATE, SEQ_NUMBER DECIMAL(13, 0) )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table PRS_VERTICALS_text 
select * from PRS_VERTICALS;

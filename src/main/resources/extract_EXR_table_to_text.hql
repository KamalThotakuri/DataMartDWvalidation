--use onetimeload;
drop table if exists EMAIL_XREF_text;
CREATE TABLE IF NOT EXISTS EMAIL_XREF_text(BIN DECIMAL(13, 0), PIN DECIMAL(13, 0), EXR_email_address STRING, EXR_ADD_DT STRING, EXR_UPD_DT STRING )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table EMAIL_XREF_text 
select * from EMAIL_XREF;

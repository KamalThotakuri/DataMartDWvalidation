--use onetimeload;


drop table if exists BUREAU_ID_KNOWLEDGEBASE_text;
CREATE TABLE IF NOT EXISTS BUREAU_ID_KNOWLEDGEBASE_text (list_code STRING, file_date DATE, pin DECIMAL(13,0), consumer_link STRING, bureau_id_number STRING, bureau_ind STRING, bureau_match_key STRING, ssn STRING, address_link STRING, bureau_seq_number STRING )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table BUREAU_ID_KNOWLEDGEBASE_text 
select * from BUREAU_ID_KNOWLEDGEBASE;







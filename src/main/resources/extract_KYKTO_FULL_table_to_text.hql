--use onetimeload;
drop table if exists KTO_BIN_SITEBIN_XREF_FULL_text;
CREATE TABLE IF NOT EXISTS KTO_BIN_SITEBIN_XREF_FULL_text(BIN DECIMAL(13, 0), KYKTO_addr_type STRING, KYKTO_bus_name_type STRING, KYKTO_prior_record_flag STRING, KYKTO_site_bin DECIMAL(24, 0), KYKTO_addr_key STRING, KYKTO_addr_rank DECIMAL(8, 0), KYKTO_multi_site_type STRING, KYKTO_multi_site_bin DECIMAL(14, 0), KYKTO_BUSNA_FLAG STRING, RSID_ENTITY DECIMAL(13, 0), RSID DECIMAL(13, 0))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table KTO_BIN_SITEBIN_XREF_FULL_text 
select * from KTO_BIN_SITEBIN_XREF_FULL;

--use onetimeload;
drop table if exists BUS_RET_MAIL_MASTERFILE_text;
CREATE TABLE IF NOT EXISTS BUS_RET_MAIL_MASTERFILE_text (bin DECIMAL(13, 0), record_type_id DECIMAL(1, 0), serial_number DECIMAL(8, 0), mailer_id_code STRING, unique_mailpiece_id_num DECIMAL(16, 0), move_efct_dt DECIMAL(6, 0), move_type STRING, deliverability_code STRING, postal_service_site_id DECIMAL(3, 0), coa_name STRING, old_address_type STRING, old_urbanization_name STRING, parsed_old_address STRING, old_city_state_zip STRING, new_address_type STRING, new_urbanization_name STRING, parsed_new_address STRING, new_city_state_zip STRING, label_format_new_address STRING, filler STRING, postage_due DECIMAL(4, 0), pmb_info STRING, class_notification_type STRING, intelligent_mail_barcode DECIMAL(32, 0), filler2 STRING, amex_rsvp STRING, add_dt DATE, pin DECIMAL(13, 0), first_name STRING, last_name STRING, address1 STRING, address2 STRING, city STRING, state STRING, zipcode STRING, zip4 STRING, business_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table BUS_RET_MAIL_MASTERFILE_text 
select * from BUS_RET_MAIL_MASTERFILE;

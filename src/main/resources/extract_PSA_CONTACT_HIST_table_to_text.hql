--use onetimeload;
drop table if exists PSA_CONTACT_HIST_text;
CREATE TABLE IF NOT EXISTS PSA_CONTACT_HIST_text(CNTID_HOUSEHOLD_ID STRING, CNTID_cons_link STRING, CNTID_mld_cons_link STRING, CNTID_cons_addr_link STRING, CNTID_PIN DECIMAL(13, 0), CNTID_MLD_PIN DECIMAL(13, 0), CNTID_first_name STRING, CNTID_last_name STRING, CNTID_addr1 STRING, CNTID_addr2 STRING, CNTID_city STRING, CNTID_state STRING, CNTID_zipcode STRING, CNTID_zip4 STRING, CNTPS_psa_DBUID DECIMAL(10, 0), CNTPS_psa_cell_id STRING, CNTPS_psa_prim_src_code STRING, CNTPS_psa_prim_spid STRING, CNTPS_psa_ia_code_1 STRING, CNTPS_psa_web_prim_src_code STRING, CNTPS_psa_web_spid STRING, CNTPS_psa_fee_code STRING, CNTPS_psa_template_code STRING, CNTPS_psa_ibtm_src_code STRING, CNTPS_psa_ibtm_spid STRING, CNTPS_psa_expiration_dt DATE, CNTPS_psa_waveid STRING, CNTPS_psa_partition_code STRING, CNTPS_psa_layout_code STRING, CNTPS_psa_vendor_code STRING, CNTPS_psa_mail STRING, CNTPS_psa_ibtm STRING, CNTPS_psa_web_app STRING, CNTPS_psa_obtm STRING, CNTPS_psa_mail_dt DATE, CNTPS_psa_campaign_type STRING, CNTPS_psa_offer_code STRING, CNTPS_psa_sch_opt_date STRING, CNTPS_psa_sch_ship_date STRING, CNTPS_psa_sch_dtp_date STRING, CNTPS_email STRING, CNTPS_email_prim_src_code STRING, CNTPS_email_spid STRING, CNTPS_email_address STRING, CNTPS_email_type STRING, CNTPS_deliverytype STRING, CNTPS_deliver_ind STRING, CNTPS_dsf_delivery_type STRING, CNTPS_ncoa_efct_dt DATE, CNTPS_cdi_vacancy_ind STRING, CNTPS_lacaddr_only_link STRING, CNTPS_lachouseholdid STRING, CNTPS_recordmatchcode STRING, CNTPS_pasaafootnotes STRING, CNTPS_record_source STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '-2'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:out}';

set  mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx2450m;

insert overwrite table PSA_CONTACT_HIST_text 
select * from PSA_CONTACT_HIST;

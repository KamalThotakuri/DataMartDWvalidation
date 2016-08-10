#!/bin/bash

# command to invoke 


echo $# arguments passed
if [ $# -ne 2 ]; then 
    echo "Illegal number of parameters, this script requires 3 arguments <hive_csv_serde_jar> <udf_jar> <out_data_dir> <tweets_data_dir> <stop_words_file> <queue_name>"
	exit 1
fi

tables_to_validate=$1
bawdw_table_extract_location=$2
queue_name=$3
IFS=,
ary=($tables_to_validate)
# use current directory as output


#***************************************** Start of BIN table ***************************************************************
# extract BIN table data 
datamart_folder_name=Data/DATAMART/
datamart_folder_path="$bawdw_table_extract_location$datamart_folder_name"

for key in "${!ary[@]}"; do
extractlocation="$datamart_folder_path${ary[$key]}"
echo "$extractlocation"
echo "./scripts/extract_${ary[$key]}_table_to_text.hql"
	if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi  
	 
	if [ $? -ne 0 ]; then
			echo "1failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "1failed to create $extractlocation, exiting the script"
			exit -1
	fi
     hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f "./scripts/extract_${ary[$key]}_table_to_text.hql"
	if [ $? -ne 0 ]; then
			echo "1failed to extract BIN table data, exiting the script"
			exit -1
	fi
		# MR job for  BIN table data validation		
		hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dmart.dataloadvalidation.DmartValidatorRunner ${ary[$key]}

	if [ $? -ne 0 ]; then
			echo "1failed to execute the validation of BIN table, exiting the script"
			exit -1
	fi
	
	#  job for consolidate zip table  validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.consolidate.results.ResultsRunner  ${ary[$key]}
	if [ $? -ne 0 ]; then
			echo "1failed to execute the validation of BIN table, exiting the script"
			exit -1
	fi

done

#***************************************** end for BIN table *****************************************************************


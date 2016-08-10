package com.acxiom.pmp.mr.dmart.dataloadvalidation;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWConfiguration;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;
public class DmartValidatorRunner extends Configured implements Tool,DWConfigConstants{
	private static Logger log = LoggerFactory.getLogger(DmartValidatorRunner.class);

	@Override
	public int run(String[] args) throws Exception {
        String tabletoCompare = args[0];
		String configfileName = "dmartdwconfig.properties";
		String path = "config" + FSEP + configfileName;
		DWConfiguration.loadProps(path);
		Properties prop =  DWConfiguration.getProps();	
		String tablePropfileName = "dmart.table.properties";
		String tableProppath = "config" + FSEP + tablePropfileName;	
		DWConfiguration.loadProps(tableProppath);
		Properties tableprop =  DWConfiguration.getProps();
		String tablePropName = tabletoCompare.toLowerCase().replace(UNDERSCORE, SINGLEDOT) + ROW_KEY_NAME;
		System.out.println("PropertyNaem@" + tablePropName);		
		String qtColNames = tableprop.getProperty(tabletoCompare.toLowerCase().replace(UNDERSCORE, DOT) + QUOTE_COLS_NAME);
		
		//creating configuration 
		Configuration conf= new Configuration();
		conf.set("mapreduce.job.reduces", "60");
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		//conf.set(DWVALIDATION_SOURCE_QUOTES_TABLES, prop.getProperty(DWVALIDATION_SOURCE_QUOTES_TABLES));		
		conf.set(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_SOURCE_TABLES_DATA_LOCATON));
		conf.set(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON));	
		String csTargetHeader = DWUtil.getTargetHeaderColumns(prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) ,tabletoCompare);
		conf.set(DWVALIDATION_TARGET_HEADER, csTargetHeader);
		String dateColIndexs = DWUtil.getDatecolIndx(csTargetHeader);
		conf.set(DATE_COL_INDEXS, dateColIndexs);
		String sourceInputDataSet = DWUtil.parseSoruceTableInput(prop.getProperty(DWVALIDATION_START_DATAE), 
				prop.getProperty(DWVALIDATION_END_DATAE), 
				tabletoCompare,
				prop.getProperty(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON), 
				prop.getProperty(DWVALIDATION_SOURCE_EXCLUDED_TABLES));
		String dwTableInputDataSet = prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) + DATA_FOLDER_NAME + FSEP +  DMART_FOLDER_NAME+ FSEP+ tabletoCompare ;
		Properties headerFileProps = DWUtil.getSourceHeaderFiles(sourceInputDataSet);
		String headerFilesStr = DWUtil.getSourceHeaderColumns(headerFileProps);
		conf.set(DWVALIDATION_SOURCE_HEADERS, headerFilesStr);		
		//conf.set(DWVALIDATION_ROW_KEY,prop.getProperty(DWVALIDATION_ROW_KEY));
		String rwKey = tableprop.getProperty(tablePropName);
		String outPath = prop.getProperty(DWVALIDATION_RESULT_LOCATION ) + tabletoCompare + FSEP + 
				                              prop.getProperty(DWVALIDATION_START_DATAE) + UNDERSCORE + prop.getProperty(DWVALIDATION_END_DATAE);
		conf.set(DWVALIDATION_RESULT_LOCATION, outPath);
		//System.out.println("Result Location========" + outPath);		
		conf.set(DWVALIDATION_COL_SAMPLING_COUNT, prop.getProperty(DWConfigConstants.DWVALIDATION_COL_SAMPLING_COUNT));
		conf.set(DWVALIDATION_DETAILED_RESULT, prop.getProperty(DWVALIDATION_DETAILED_RESULT));
		conf.set(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE, args[0]);
		//conf.set(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE));
		conf.set(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE, tabletoCompare);
		conf.set(DWVALIDATION_ROW_KEY,rwKey);
		if(qtColNames != null){
			conf.set(DWVALIDATION_SOURCE_QUOTES_TABLES, qtColNames);
		}
		

		
		//System.out.println("From Outsideeeeeeeeeee:" + conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE));
		/////temp code need to delete
		
		Map<String, Map<String, String>> map1 = DWUtil.getHeadersAsMap(headerFilesStr);
		for(Map.Entry<String, Map<String, String>> elements:map1.entrySet() ){

			 System.out.println(elements.getKey());
			  Map<String, String> map2 = elements.getValue();
			  for(Map.Entry<String, String>datecoulmns:map2.entrySet() ){
				  System.out.println("Date:" + datecoulmns.getKey());
				  System.out.println("ColumnIndex:" + datecoulmns.getValue());
			  }
		}  
		/*
		Map<String, Map<String, String>> dateColIndxMap = new HashMap<String, Map<String, String>>();

	    dateColIndxMap = DWUtil.getDatecolIndx(map1);
		*/
		
		Job job = new Job(conf, "DWValidation");
		job.setJarByClass(DmartValidatorRunner.class);
		job.setMapperClass(ValidatorMapper.class);
		job.setReducerClass(ValidatorReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);			
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(NullWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//job.setNumReduceTasks(0);
		// inputs
		job.setInputFormatClass(TextInputFormat.class);
		String inputDataSet = sourceInputDataSet + COMMA + dwTableInputDataSet;
		System.out.println("FullInputData: " +inputDataSet);
		System.out.println("Hive table to compare:" +conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE) );
		System.out.println("source Data location:" +conf.get(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON) );
		System.out.println("Row Key is :" +conf.get(DWVALIDATION_ROW_KEY) );
		System.out.println("Row Key@" + rwKey);
		//log.info("Input Path:"+inputDataSet);
		FileInputFormat.setInputPaths(job, inputDataSet);

		// output
		//job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			outputPath.getFileSystem(conf).delete(outputPath,true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		log.info("Submitting the job");
		boolean b = job.waitForCompletion(true);
		System.out.println("b is "+b);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DmartValidatorRunner vr = new DmartValidatorRunner();
		int res = ToolRunner.run(conf, vr, args);
		System.exit(res);       
	}
}
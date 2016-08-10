package com.acxiom.pmp.mr.dmart.dataloadvalidation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWException;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;

public class ValidatorReducerBak extends Reducer< Text, Text, Text, NullWritable> implements DWConfigConstants {
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private Map<String, Boolean> targetHiveTableFlagMap = new HashMap<String, Boolean>();
	private static Logger log = LoggerFactory.getLogger(ValidatorReducerBak.class);
	private static Map<String, Integer> diffReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> sourceReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> targetReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> matchedReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> sourceRowsReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> targetRowsReportingColLimit = new HashMap<String, Integer>();


	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable;
	private String csTargetHeader;
	private String srcRequiredTable;
	private MultipleOutputs<Text, NullWritable> out;
	private String rootOutputLoc;
	private int limitCount;
	private String sourceDataLocation=null;
	private Boolean resultType = null;

	//make it local after debugging
	String fType=null;

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
		targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		csTargetHeader = conf.get(DWVALIDATION_TARGET_HEADER);
		srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
		rootOutputLoc = conf.get(DWVALIDATION_RESULT_LOCATION);
		String limit = conf.get(DWVALIDATION_COL_SAMPLING_COUNT);
		limitCount = Integer.parseInt(limit); 
		sourceDataLocation = conf.get(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON);
		resultType = Boolean.valueOf(conf.get(DWVALIDATION_DETAILED_RESULT));
		limitCount = limitCount %9;
		map = DWUtil.getHeadersAsMap(headerFiles);
		out = new MultipleOutputs(context);		

	}


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException{

		Map<String, String> targetHiveTableMap = new HashMap<String, String>();
		Map<String, String> soruceTableMap = new HashMap<String, String>();


		//log.info("reducer messaged from log4j");
		int itrDepth=0;
		boolean targetTableExist=false;
		boolean srcTableExist=false;
		int targetCount =0;
		String targetFilePath = null;
		String srcFilePath = null;
		for(Text tableDateData : values){
			String[] tableDateDataArr = tableDateData.toString().split(APPENDER,4);
			String tableName = tableDateDataArr[0];
			String date = tableDateDataArr[1];
			String inFileName = tableDateDataArr[2];
			//String mapperSplitValue = tableDateDataArr[3];
			String record = tableDateDataArr[3];
			//fType = tableDateDataArr[4];
			//declaration has to move to try block
			String[] scolValues =null;
			String[] scols = null;
			String sheader = null;
			if(inFileName.contains(sourceDataLocation)){
				try{
					srcTableExist=true;
					srcFilePath = inFileName;
					Map<String, String> dateHeader = map.get(tableName);
					sheader = dateHeader.get(date);
					scols = sheader.split(COMMA);
					//String[] colValues = record.split(TAB,-2);
					String nwrecord = new String(record.getBytes(),
							0, record.length(), 
							Charsets.ISO_8859_1);
					scolValues = nwrecord.split(THORN,-2);
					if(scolValues.length == scols.length) {
						for(int i = 0; i < scols.length; i++) {
							String sCName = scols[i].toUpperCase();						
							String scValue = scolValues[i];
							scValue = scValue.replace(QUOTES, EMPTY);
							soruceTableMap.put(sCName, scValue + TILD + inFileName );	
							/*ss.append(colValues[i]);
						ss.append(";")*/;
						}
					}else {
						throw new Exception();
					}

				}catch(Exception e){
					//e.printStackTrace();
					String exTrace = DWUtil.getStackTraceAsString(e);
					log.error("Error occured while validating the source col values with its header"+e.getMessage());
					log.error(exTrace);
					throw new DWException("Count Didn't Match " + 
							"headerLength:"+ scols.length + "\n"+
							"recordlength "+ scolValues.length + "\n"+ 
							"TableName: " +tableName  + "\n"+
							"InputFileName:" + inFileName +"\n" +
							" Record:" + record , e);
				}
			}else{
				/// Loading of Target HashMap
				targetTableExist=true;
				targetFilePath= inFileName;
				String[] tcols = csTargetHeader.split(COMMA);
				String nwrecord = new String(record.getBytes(),
						0, record.length(), 
						Charsets.ISO_8859_1);
				String[] tcolValues = nwrecord.split(THORN,-2);		
				try{
					if(tcols.length == tcolValues.length ){
						for(int i = 0; i < tcols.length; i++) {
							String tcName = tcols[i].toUpperCase();						
							String tcValue = tcolValues[i];
							tcValue = tcValue.replace(QUOTES, EMPTY);						
							targetHiveTableMap.put(tcName, tcValue + TILD + inFileName );
							targetHiveTableFlagMap.put(tcName, false);
						}
					}else {

						log.error("col length and headerlenth didn't match col header Length" + + tcols.length  + "\n" + "rowLength" + tcolValues.length );						
						throw new DWException("col length and headerlenth didn't match col header Length" + tcols.length  + "\n" + "rowLength" + tcolValues.length );
					}
				}catch(Exception e){
					//e.printStackTrace();
					String exTrace = DWUtil.getStackTraceAsString(e);
					log.error("Error occured while validating the target col values with its header "+e.getMessage());
					log.error(exTrace);
					throw new DWException("Count Didn't Match " + "ccount: " + tcols.length + " cVcount:" + tcolValues.length + "TableName: " + tableName + 
							" Record:" + record +							
							//" MapperSplitValue:" + mapperSplitValue +
							" InputFileName:" + inFileName, e);
				}
			}
			++itrDepth;
		}


		try{
			/*Checks no of value iterations for a row is greater than one and iteration has both target and source tables */
			if(targetTableExist && srcTableExist){
				if(targetHiveTableMap.size() != soruceTableMap.size()){
					throw new DWException("Size of the source reocrd and target record didn't match \n" + "Source Map size:" + soruceTableMap.size() 
					+ "\n Target Map size:" + targetHiveTableMap.size() );
				}else{
					for (String colName : soruceTableMap.keySet()) {
						String targetColValueFileName = targetHiveTableMap.get(colName);
						String sourceColValueFilName = soruceTableMap.get(colName);
						String[] sourceValues = sourceColValueFilName.split(TILD);
						String sourceColVal = sourceValues[0];
						String sourceFileName = sourceValues[1];
						if(targetColValueFileName !=null ){
							String[] targetValues = targetColValueFileName.split(TILD);
							String targeColVal = targetValues[0];
							String targetFileName = targetValues[1];
							if (sourceColVal.isEmpty() && targeColVal.equals(ORC_NULL)){
								String sKvalue =key.toString();
								String sDiff = appendMatchedColResult(colName, sourceColVal, targeColVal, sKvalue, sourceFileName,  targetFileName);
								keyOut.set(sDiff);
								String resultLocation = rootOutputLoc + FSEP + MATCHED_COLS;
								if(resultType){
									//out.write(keyOut, NullWritable.get(), resultLocation);		
								}							
								targetHiveTableFlagMap.put(colName, true);
								int currentValue =0;
								String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  MATCHED_COLS + UNDERSCORE +DWVALIDATION_COL_SAMPLING_COUNT ;
								if(matchedReportingColLimit.get(colName) == null){
									matchedReportingColLimit.put(colName, 0);													
									keyOut.set(sDiff);									
									out.write(keyOut, NullWritable.get(), smplingResultLocation);	
								}else{
									currentValue = matchedReportingColLimit.get(colName);
									if(currentValue<limitCount){
										keyOut.set(sDiff);									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
										matchedReportingColLimit.put(colName, ++currentValue);
										int temp = matchedReportingColLimit.get(colName);
										if(temp>10){
											throw new DWException("Looping more time" + Integer.toString( temp));
										}
									}
								}
							}else{
								if(!sourceColVal.equals(targeColVal)){
									String sKvalue =key.toString();
									String sDiff = appendDifferedResult(colName, sourceColVal, targeColVal, sKvalue, sourceFileName,  targetFileName);
									keyOut.set(sDiff);
									String resultLocation = rootOutputLoc + FSEP + DIFFERED_COLS;
									if(resultType){
										out.write(keyOut, NullWritable.get(), resultLocation);		
									}
									targetHiveTableFlagMap.put(colName, true);
									int currentValue =0;
									String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  DIFFERED_COLS + UNDERSCORE +DWVALIDATION_COL_SAMPLING_COUNT ;
									if(diffReportingColLimit.get(colName) == null){
										diffReportingColLimit.put(colName, 0);													
										keyOut.set(sDiff);									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
									}else{
										currentValue = diffReportingColLimit.get(colName);
										if(currentValue<limitCount){
											keyOut.set(sDiff);									
											out.write(keyOut, NullWritable.get(), smplingResultLocation);	
											diffReportingColLimit.put(colName, ++currentValue);
											int temp = diffReportingColLimit.get(colName);
											if(temp>10){
												throw new DWException("Looping more time" + Integer.toString( temp));
											}
										}
									}
								}else{
									String sKvalue =key.toString();
									String sDiff = appendMatchedColResult(colName, sourceColVal, targeColVal, sKvalue, sourceFileName,  targetFileName);
									keyOut.set(sDiff);
									String resultLocation = rootOutputLoc + FSEP + MATCHED_COLS;								
									if(resultType){
										//out.write(keyOut, NullWritable.get(), resultLocation);		
									}
									targetHiveTableFlagMap.put(colName, true);
									int currentValue =0;
									String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  MATCHED_COLS + UNDERSCORE +DWVALIDATION_COL_SAMPLING_COUNT ;
									if(matchedReportingColLimit.get(colName) == null){
										matchedReportingColLimit.put(colName, 0);													
										keyOut.set(sDiff);									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
									}else{
										currentValue = matchedReportingColLimit.get(colName);
										if(currentValue<limitCount){
											keyOut.set(sDiff);									
											out.write(keyOut, NullWritable.get(), smplingResultLocation);	
											matchedReportingColLimit.put(colName, ++currentValue);
											int temp = matchedReportingColLimit.get(colName);
											if(temp>10){
												throw new DWException("Looping more time" + Integer.toString( temp));
											}
										}
									}
								}
							}
						}else if(targetHiveTableMap.get(colName) ==null ){
							String soruceDiff = appendSourceResult(colName, sourceColVal, sourceFileName);
							keyOut.set(soruceDiff);
							String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS + "_ntintmap" ;
							if(resultType){
								out.write(keyOut, NullWritable.get(), resultLocation);		
							}
							///Printing limit output
							int currentValue =0;
							String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_SOURCE_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
							if(sourceReportingColLimit.get(colName) == null){
								sourceReportingColLimit.put(colName, 0);
								out.write(keyOut, NullWritable.get(), smplingResultLocation);	
							}else{
								currentValue = sourceReportingColLimit.get(colName);
								if(currentValue<limitCount){
									out.write(keyOut, NullWritable.get(), smplingResultLocation);	
									sourceReportingColLimit.put(colName, currentValue +1);
									int temp = sourceReportingColLimit.get(colName);
									if(temp>limitCount){
										throw new DWException("Looping more time" + Integer.toString( temp));
									}
								}
							}

						}
					}
				}

			}else if(srcTableExist && !targetTableExist){
				String keyValue = key.toString();
				String soruceDiff = appendSourceResultRows(keyValue,  srcFilePath);
				keyOut.set(soruceDiff);
				String resultLocation = rootOutputLoc + FSEP + "Rows"+ EXISTS_ONLY_IN_SOURCE_COLS  ;
				if(resultType){
					out.write(keyOut, NullWritable.get(), resultLocation);		
				}
				///Printing limit output
				int currentValue =0;
				String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP + "Rows"+ EXISTS_ONLY_IN_SOURCE_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
				if(sourceRowsReportingColLimit.get(keyValue) == null){
					sourceRowsReportingColLimit.put(keyValue, 0);
					out.write(keyOut, NullWritable.get(), smplingResultLocation);	
				}else{
					currentValue = sourceRowsReportingColLimit.get(keyValue);
					if(currentValue<limitCount){
						out.write(keyOut, NullWritable.get(), smplingResultLocation);	
						sourceRowsReportingColLimit.put(keyValue, currentValue +1);
						int temp = sourceRowsReportingColLimit.get(keyValue);
						if(temp>limitCount){
							throw new DWException("Looping more time" + Integer.toString( temp));
						}
					}
				}

			}else if(targetTableExist && !srcTableExist ){
				String keyValue = key.toString();
				String soruceDiff = appendTargetResultRows(keyValue,  targetFilePath);
				keyOut.set(soruceDiff);
				String resultLocation = rootOutputLoc + FSEP + "Rows"+ EXISTS_ONLY_IN_TARGET_COLS  ;
				if(resultType){
					out.write(keyOut, NullWritable.get(), resultLocation);		
				}
				///Printing limit output
				int currentValue =0;
				String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP + "Rows"+ EXISTS_ONLY_IN_TARGET_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
				if(targetRowsReportingColLimit.get(keyValue) == null){
					targetRowsReportingColLimit.put(keyValue, 0);
					out.write(keyOut, NullWritable.get(), smplingResultLocation);	
				}else{
					currentValue = targetRowsReportingColLimit.get(keyValue);
					if(currentValue<limitCount){
						out.write(keyOut, NullWritable.get(), smplingResultLocation);	
						targetRowsReportingColLimit.put(keyValue, currentValue +1);
						int temp = targetRowsReportingColLimit.get(keyValue);						
					}
				}
			}

			if(targetTableExist && srcTableExist){
				if(targetHiveTableFlagMap.size() >0){
					for(String colName:targetHiveTableFlagMap.keySet()){				
						try{	
							if(!(targetHiveTableFlagMap.get(colName))){
								String[] targetValues = targetHiveTableMap.get(colName).split(TILD);
								String targeColVal = targetValues[0];
								String targetFileName = targetValues[1];
								/*if(targeColVal=="null"){
							updatetoNull.append(colName);
							updatetoNull.append(TAB);
							updatetoNull.append(key.toString());								
						}else{*/
								String targetDiff = appendTargetResult(colName,  targetFileName);
								keyOut.set(targetDiff);							
								String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_TARGET_COLS;
								if(resultType){
									//out.write(keyOut, NullWritable.get(), resultLocation);		
								}								
								//prinitng limit output
								int currentValue =0;
								String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_TARGET_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
								if(targetReportingColLimit.get(colName) == null){
									targetReportingColLimit.put(colName, 0);
									out.write(keyOut, NullWritable.get(), smplingResultLocation);	
								}else{
									currentValue = targetReportingColLimit.get(colName);
									if(currentValue<limitCount){
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
										targetReportingColLimit.put(colName, ++currentValue);
										int temp = targetReportingColLimit.get(colName);
										if(temp>10){
											throw new DWException("Looping more time" + Integer.toString( temp));
										}
									}
								}
							}
						}catch(Exception e){
							throw new DWException("Error occured while wriring Exist only targets section"  , e);
						}
					}
				}
			}
		}catch (IOException | InterruptedException e) {
			//e.printStackTrace();
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while reading targetheaderfile "+e.getMessage());
			log.error(exTrace);
		}
	}

	private String appendTargetResultRows(String keyValue, String targetFilePath) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append(keyValue);
		sb.append(COMMA);
		sb.append(targetFilePath);
		return sb.toString();
	}


	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		//System.out.println("Required Tables:" + srcRequiredTable);
		out.close();

	}

	private static String appendDifferedResult(String colName, String sCval, String tCval, String key, String sFile, String tFile){
		StringBuilder sb = new StringBuilder();
		sb.append(colName);
		sb.append(COMMA);
		sb.append(sCval);
		sb.append(COMMA);
		sb.append(tCval);
		sb.append(COMMA);
		sb.append(key.toString());
		sb.append(COMMA);
		sb.append(sFile);
		sb.append(COMMA);
		sb.append(tFile);
		return sb.toString();
	}

	private static String appendSourceResult(String colName, String sourceValue, String sFile){
		StringBuilder sb = new StringBuilder();
		sb.append(colName);
		sb.append(COMMA);
		sb.append(sourceValue);
		sb.append(COMMA);
		sb.append(sFile);
		return sb.toString();
	}

	private static String appendSourceResultRows(String key,String sFile){
		StringBuilder sb = new StringBuilder();
		sb.append(key);
		sb.append(COMMA);
		sb.append(sFile);
		return sb.toString();
	}	

	private static String appendTargetResult(String colName, String tFile){
		StringBuilder sb = new StringBuilder();
		sb.append(colName);
		sb.append(COMMA);
		sb.append(tFile);
		return sb.toString();

	}

	private static String appendMatchedColResult(String colName, String sCval, String tCval, String key, String sFile, String tFile){
		StringBuilder sb = new StringBuilder();
		sb.append(colName);
		sb.append(COMMA);
		sb.append(sCval);
		sb.append(COMMA);
		sb.append(tCval);
		sb.append(COMMA);
		sb.append(key.toString());
		sb.append(COMMA);
		sb.append(sFile);
		sb.append(COMMA);
		sb.append(tFile);
		return sb.toString();
	}

}
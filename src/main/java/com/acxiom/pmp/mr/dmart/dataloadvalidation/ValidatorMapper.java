
package com.acxiom.pmp.mr.dmart.dataloadvalidation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWException;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;
//Comment added 
public class ValidatorMapper extends Mapper<LongWritable, Text, Text, Text > implements DWConfigConstants{
	private String date;
	private String tableName;
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable; 
	private int primaryKeyIndex=0;
	private boolean isCompositeKey = false;
	private String srcRequiredTable;
	private String rowKeyCols;
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private Map<String, Map<String, String>> dateColIndxMap = new HashMap<String, Map<String, String>>();
	private ArrayList<Integer> compositeKeyIndex = new ArrayList<Integer>();
	private String dateColIndxs;	
	private String inputFilePath;
	private String inputFileName;
	private String sourceDataLocation;
	private String lstName;
	private String quotetables=null;
	private boolean sourceTableFlag=false;
	private String csTargetHeader;
	private static Logger log = LoggerFactory.getLogger(ValidatorMapperBak.class);
	private static int targetColHeaderLength=0;
	private static int srcColLength=0;

	private static String printkey="";

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		try {
			inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();	
			quotetables = conf.get(DWVALIDATION_SOURCE_QUOTES_TABLES);
			sourceDataLocation = conf.get(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON);
			targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE) ;
			//targetHiveTable = "BNKTO_FULL" ;
			srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
			String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
			dateColIndxs = conf.get(DATE_COL_INDEXS);
			map = DWUtil.getHeadersAsMap(headerFiles);
			rowKeyCols = conf.get(DWVALIDATION_ROW_KEY);
			csTargetHeader = conf.get(DWVALIDATION_TARGET_HEADER);
			if(rowKeyCols.split(COMMA).length >1){
				isCompositeKey=true;
			}
			if(inputFilePath.contains(sourceDataLocation)){
				sourceTableFlag=true;
				String[] nameHolder = inputFileName.split(UNDERSCORE);
				String[] tableNameHolder = inputFileName.split(TABLE_NAME_SPLITTER_FROM_FNAME);
				tableName = tableNameHolder[0];
				int index = nameHolder.length-1;
				lstName = nameHolder[index];
				//lastName:20160531.tsv
				String[] dateHolder = lstName.split(DOT);
				date = dateHolder[0];
				if(!isCompositeKey){
					Map<String, String> dateHeader = map.get(tableName);
					String header = dateHeader.get(date);
					String[] cols = header.split(COMMA);
					srcColLength = cols.length;
					for(int i = 0; i < cols.length; i++) {
						if(cols[i].toUpperCase().equals(rowKeyCols.toUpperCase())){
							primaryKeyIndex= i;
						}
					}
				}else{
					//String[] keyColumns = rowKeyCols.split(COMMA);
					Map<String, String> dateHeader = map.get(tableName);
					String header = dateHeader.get(date);
					String[] cols = header.split(COMMA);
					srcColLength = cols.length;
					String[] keyCols = rowKeyCols.split(COMMA);
					for(String kcol:keyCols){
						innerloop:
							for(int i = 0; i < cols.length; i++) {
								if(cols[i].toUpperCase().equals(kcol.toUpperCase())){
									compositeKeyIndex.add(i);									
									//break innerloop;
								}
							}
					}
				}	
			}
			if (!inputFilePath.contains(sourceDataLocation)){
				tableName = targetHiveTable;
				String[] cols = csTargetHeader.split(COMMA);
				targetColHeaderLength= cols.length;
				if(isCompositeKey){					
					String[] keyCols = rowKeyCols.split(COMMA);
					for(String kcol:keyCols){
						innerloop:
							for(int i = 0; i < cols.length; i++) {
								if(cols[i].toUpperCase().equals(kcol.toUpperCase())){
									compositeKeyIndex.add(i);
									//break innerloop;
								}
							}
					}
				}else{

					for(int i = 0; i < cols.length; i++) {
						if(cols[i].toUpperCase().equals(rowKeyCols.toUpperCase())){
							primaryKeyIndex= i;
						}
					}
				}
			}
			
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new DWException("row Keys are "+ rowKeyCols + LSEP + "TableName is:" + tableName  + LSEP + "targettable:" + targetHiveTable + LSEP
					+ "fileName" + inputFileName,e1);
			
		}  
	}

	class DataRecord {
		String rowKey;
		String completeRecord;

		public DataRecord(String primaryKey, String completeRecord) {
			this.rowKey = primaryKey;
			this.completeRecord = completeRecord;
		}

		public String getRowKey() {
			return rowKey;
		}

		public String getRecord() {
			return completeRecord;
		}
	}

	private DataRecord handlePrimarKey(Text value) {
		boolean delimCheck=false;
		try{
			StringBuilder result = new StringBuilder();
			String primaryKey = null;
			if(!sourceTableFlag){
				//if(tableName.equals(targetHiveTable) ){
				//String[] columns = value.toString().split(THORN,-2);
				String data = new String(value.getBytes(),
						0, value.getLength(), 
						Charsets.ISO_8859_1);
				String[] columns = data.split(THORN,-2);
				primaryKey = columns[primaryKeyIndex];        
				for(int colIdx=0; colIdx<columns.length; colIdx++) {
					result.append(columns[colIdx].trim()+THORN);
				}
				if(columns.length > targetColHeaderLength){
					delimCheck=true;
				}

			}else{
				String line = value.toString();
				String[] columns = line.split(TAB,-2);
				primaryKey = columns[primaryKeyIndex];   
				if(quotetables != null && quotetables.contains(tableName)){					
					if(line.contains(QUOTES)){
						String[] qtindex= quotetables.split(SINGLE_COLON);
						int qtpoint = Integer.parseInt(qtindex[1]) -2; 
						for(int i=0 ; i <=qtpoint ;i++ ){
							result.append(columns[i]);
							result.append(TAB);
						}
						line = line.replace(result.toString(), "");	
						result.setLength(0);
						for(int i=0 ; i <=qtpoint ;i++ ){
							result.append(columns[i]);
							result.append(THORN);
						}
						int loccurnace = line.lastIndexOf(QUOTES);
						String quotestring = line.substring(0, loccurnace+1);
						//quotestring = quotestring.replace(TAB, THORN);
						String secondset = line.substring(loccurnace +1);
						secondset = secondset.replace(TAB, THORN);
						result.append(quotestring);
						result.append(secondset);
						String[] colcheck = result.toString().split(THORN);
						if(colcheck.length > srcColLength){
							delimCheck=true;
						}

					}else{
						for(int colIdx=0; colIdx<columns.length; colIdx++) {
							result.append(columns[colIdx].trim()+THORN);
						}
						if(columns.length > srcColLength){
							delimCheck=true;
						}
					}
				}else{
					for(int colIdx=0; colIdx<columns.length; colIdx++) {
						result.append(columns[colIdx].trim()+THORN);
					}
					if(columns.length > srcColLength){
						delimCheck=true;
					}
				}
			}
			if(result.length() > 0) {
				if(delimCheck){
					result.setLength(result.length()-2);
				}else{
					result.setLength(result.length()-1);
				}
			}
			return new DataRecord(primaryKey, result.toString());

		}catch(Exception e){
			throw new DWException("From catch:", e);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//TempDelete
		//String[] columnValues = value.toString().split(TAB,-2);
		int targetsplicount=0; 
		String srecord=null;
		try{	
			DataRecord record = null;
			if(isCompositeKey) {
				record = handleCompositeKey(value);
			} else {
				record = handlePrimarKey(value);
			}
			
			StringBuilder sb = new StringBuilder();
			if (inputFilePath.contains(sourceDataLocation)){
				sb.append(tableName);
			}else{
				sb.append(tableName);
			}

			sb.append(APPENDER);

			if (inputFilePath.contains(sourceDataLocation)){
				//This line has to keep
				sb.append(date);

				//sb.append(" Date:" + date + "targetHiveTable :" + targetHiveTable);
			}else{
				sb.append("yyyymmdd");
			}
			sb.append(APPENDER);		
			sb.append(inputFilePath);
			sb.append(APPENDER);
			//sb.append(columnValues.length);
			//sb.append(COLON);
			//sb.append(value.toString());
			sb.append(record.getRecord());
			keyOut.set(record.getRowKey());
			valueOut.set(sb.toString());
			context.write(keyOut, valueOut);
			
			/// delete the code after debugging
			String data = record.getRecord();
			String[] columns = data.split(THORN,-2);
			String primaryKey = columns[primaryKeyIndex];   
			targetsplicount = columns.length;

		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while sending the mapper output. "+e.getMessage());
			log.error(exTrace);
			throw new DWException("Keyindexes:" + printkey + LSEP + "File:" + inputFilePath  + "\n" +
					"primaryIndex:" + primaryKeyIndex + LSEP + "splitLength:" + targetsplicount + LSEP + "record" + value.toString() ,e);
		}

	}
	private DataRecord handleCompositeKey(Text value) {
		StringBuilder combiner = new StringBuilder();
		StringBuilder result = new StringBuilder();
		boolean delimCheck=false; 

		if(!sourceTableFlag){
			//String[] columns = value.toString().split(THORN,-2);
			String data = new String(value.getBytes(),
					0, value.getLength(), 
					Charsets.ISO_8859_1);
			String[] columns = data.split(THORN,-2);

			for(Integer index:compositeKeyIndex){
				String cloName = columns[index];
				combiner.append(cloName);
				combiner.append(APPENDER);
			}

			if(combiner.length() > 0) {
				combiner.setLength(combiner.length()-1);
			}			
			if(columns.length > targetColHeaderLength){
				delimCheck=true;
			}
			for(int colIdx=0; colIdx<columns.length; colIdx++) {
				result.append(columns[colIdx].trim()+THORN);
			}

		}else{
			//String[] columns = value.toString().split(THORN,-2);
			String line = value.toString();
			String[] columns = line.split(TAB,-2);
			if(quotetables != null && quotetables.contains(tableName)){					
				if(line.contains(QUOTES)){
					String[] qtindex= quotetables.split(SINGLE_COLON);
					int qtpoint = Integer.parseInt(qtindex[1]) -2; 
					for(int i=0 ; i <=qtpoint ;i++ ){
						result.append(columns[i]);
						result.append(TAB);
					}
					line = line.replace(result.toString(), "");	
					result.setLength(0);
					for(int i=0 ; i <=qtpoint ;i++ ){
						result.append(columns[i]);
						result.append(THORN);
					}
					int loccurnace = line.lastIndexOf(QUOTES);
					String quotestring = line.substring(0, loccurnace+1);
					//quotestring = quotestring.replace(TAB, THORN);
					String secondset = line.substring(loccurnace +1);
					secondset = secondset.replace(TAB, THORN);
					result.append(quotestring);
					result.append(secondset);
					String[] colcheck = result.toString().split(THORN);
					if(colcheck.length > srcColLength){
						delimCheck=true;
					}
					for(Integer index:compositeKeyIndex){
						String cloName = colcheck[index];
						combiner.append(cloName);
						combiner.append(APPENDER);
					}
					if(combiner.length() > 0) {
						combiner.setLength(combiner.length()-1);
					}
				}else{
					for(int colIdx=0; colIdx<columns.length; colIdx++) {
						result.append(columns[colIdx].trim()+THORN);
					}
					if(columns.length > srcColLength){
						delimCheck=true;
					}
					for(Integer index:compositeKeyIndex){
						String cloName = columns[index];
						combiner.append(cloName);
						combiner.append(APPENDER);
					}
					if(combiner.length() > 0) {
						combiner.setLength(combiner.length()-1);
					}
				}
			}else{
				for(int colIdx=0; colIdx<columns.length; colIdx++) {
					result.append(columns[colIdx].trim()+THORN);
				}
				if(columns.length > srcColLength){
					delimCheck=true;
				}
				for(Integer index:compositeKeyIndex){
					String cloName = columns[index];
					combiner.append(cloName);
					combiner.append(APPENDER);
				}
				if(combiner.length() > 0) {
					combiner.setLength(combiner.length()-1);
				}
			}
		}

		String rowKey = combiner.toString();
		if(result.length() > 0) {
			if(delimCheck){
				result.setLength(result.length()-2);
			}else{
				result.setLength(result.length()-1);
			}
		}
		return new DataRecord(rowKey, result.toString());
	}

}

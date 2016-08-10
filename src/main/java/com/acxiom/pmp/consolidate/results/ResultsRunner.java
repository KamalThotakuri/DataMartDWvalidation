package com.acxiom.pmp.consolidate.results;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWConfiguration;
import com.acxiom.pmp.common.DWException;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;
import com.acxiom.pmp.mr.dmart.dataloadvalidation.DmartValidatorRunner;

public class ResultsRunner extends Configured implements Tool,DWConfigConstants {
	private static Logger log = LoggerFactory.getLogger(DmartValidatorRunner.class);	
	private FileSystem fs;
	private static int limitCount=0;
	private static Map<String, Integer> diffReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> sourceReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> targetReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, String> colMap = new HashMap<String, String>();

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
	
		String tabletoCompare = args[0];
		String confIdentifier = "dmartdwconfig.properties";
		String colMappingIdentifier = "dmart.column.datatypes.mapping.properties";
		String path = "config" + FSEP + confIdentifier;
		DWConfiguration.loadProps(path);
		Properties prop =  DWConfiguration.getProps();	
		
		String resultMainFolder = prop.getProperty(DWVALIDATION_RESULT_LOCATION ) + tabletoCompare + FSEP + 
				prop.getProperty(DWVALIDATION_START_DATAE) + UNDERSCORE + prop.getProperty(DWVALIDATION_END_DATAE);
		String resultFolder = resultMainFolder + FSEP + SAMPLING_FOLDER_NAME + FSEP;
		String mergedDiffResultFile = resultFolder + MERGED_DIFFERED_COLS; 
		String mergedExistTargetResultFile = resultFolder + MERGED_EXISTS_ONLY_IN_TARGET_COLS;
		String mergedExistSrcResultFile = resultFolder + MERGED_EXISTS_ONLY_IN_SOURCE_COLS;
		limitCount = Integer.parseInt(prop.getProperty(DWConfigConstants.DWVALIDATION_COL_SAMPLING_COUNT));
		limitCount =limitCount+10;
		//Loading of col mapping properties
		String colMapfile = "config" + FSEP + colMappingIdentifier;
		DWConfiguration.loadProps(colMapfile);
		Properties colMapPropString =  DWConfiguration.getProps();	
		String colNames_Datatype= colMapPropString.getProperty(tabletoCompare);
		Properties colmapprop = getColDataTypes(colNames_Datatype);		
		for(Entry<Object, Object> entry: colmapprop.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			System.out.println("Key:" + key + " Value" + value);
			colMap.put(key.toUpperCase(), value.toUpperCase());
		}

		//Loading the result folder and looping for merging

		Path inFile = new Path(resultFolder);
		Path mrgDifResultfile = new Path(mergedDiffResultFile);
		Path mrgExistTargetfile = new Path(mergedExistTargetResultFile);
		Path mrgExistSrcfile = new Path(mergedExistSrcResultFile);
		fs = FileSystem.get(new Configuration());

		boolean exists = fs.exists(inFile);
		if(exists){
			if(fs.exists(mrgDifResultfile)){
				fs.deleteOnExit(mrgDifResultfile);
				fs.create(mrgDifResultfile,true);
			}else{
				fs.create(mrgDifResultfile,true);
			}
			if(fs.exists(mrgExistTargetfile)){
				fs.deleteOnExit(mrgExistTargetfile);
				fs.create(mrgExistTargetfile,true);
			}else{
				fs.create(mrgExistTargetfile,true);
			}
			if(fs.exists(mrgExistSrcfile)){
				fs.deleteOnExit(mrgExistSrcfile);
				fs.create(mrgExistSrcfile,true);
			}else{
				fs.create(mrgExistSrcfile,true);
			}
			FileStatus[] fileStatus = fs.listStatus(inFile);
			for(FileStatus status : fileStatus){
				if(status.isDir()){
					continue ;				
				}
				String resultFileName = status.getPath().getName().toString();
				Path  resultLimitfile = status.getPath();
				if((resultFileName.contains(DIFFERED_COLS)) && (!resultFileName.equals(MERGED_DIFFERED_COLS))){
					mergeDifferedcols(resultLimitfile, mrgDifResultfile);
				}else if((resultFileName.contains(EXISTS_ONLY_IN_SOURCE_COLS)) && (!resultFileName.equals(MERGED_EXISTS_ONLY_IN_SOURCE_COLS))){
					mergeSourceexistcols(resultLimitfile, mrgExistSrcfile );
				}else if((resultFileName.contains(EXISTS_ONLY_IN_TARGET_COLS)) && (!resultFileName.equals(MERGED_EXISTS_ONLY_IN_TARGET_COLS))) {
					mergeTartetexistcols(resultLimitfile, mrgExistTargetfile );
				}
			}
		}else{
			throw new DWException("Following result Location folder doesn't exist" +inFile );
		}


		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		ResultsRunner vr = new ResultsRunner();
		int res = ToolRunner.run(conf, vr, args);
		System.exit(res);       

	}

	private static void mergeDifferedcols(Path strinpuptFileName, Path outputFileName ) {
		String line=null;
		//Path outputfile = new Path(outputFileName);	
		//System.out.println("inputfilName======="+ strinpuptFileName);
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			if(fs.exists(outputFileName)){
				//System.out.println("exist " + outputFileName.toString());
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured while merging the diff files"+e1.getMessage());
			log.error(exTrace);
		}
		try {
			fs = FileSystem.get(new Configuration());
			FSDataInputStream is = fs.open(strinpuptFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.append(outputFileName)));
			int currentValue=0;
			LIMITLoop:
				while ((line = br.readLine()) != null){
					line = br.readLine();
					if(line !=null){
						String[] lineValues = line.split(COMMA);
						String colName = lineValues[0].toUpperCase();
						//adding if it is  first time 
						if(diffReportingColLimit.get(colName) == null){
							diffReportingColLimit.put(colName, 0);	
							String colDatatype =  colMap.get(colName);
							if(colDatatype.toUpperCase().equals(DECIMALTYPE)){
								String srcColvalue = lineValues[1];
								String trgColvale = lineValues[2];
								try{	
									BigDecimal bg1 = new BigDecimal(srcColvalue);
									BigDecimal bg2 = new BigDecimal(trgColvale);	
									int res;
									res = bg1.compareTo(bg2);	  
									if(res==0){
										continue;
									}else{
										bw.write(line);
										bw.newLine();
									}
								}catch(NumberFormatException nef){
									continue;
								}
							}else if(colDatatype.toUpperCase().equals(DATETYPE) || colName.toLowerCase().contains(DATE_COL_REFERENCE) || 
									colName.toLowerCase().contains(DATE_COL_REFERENCE_1)){
								String srcColvalue = lineValues[1];
								String trgColvale = lineValues[2];
								srcColvalue = srcColvalue.replace(HYPHEN, "");
								trgColvale = trgColvale.replace(HYPHEN, "");
								if(!trgColvale.equals(srcColvalue)){
									bw.write(line);
									bw.newLine();
								}
							}
							else{
								bw.write(line);
								bw.newLine();
							}
							// end part for adding first time 
						}else{
							//updating if it is  first time 
							currentValue = diffReportingColLimit.get(colName);
							if(currentValue<limitCount ){

								String colDatatype =  colMap.get(colName);
								System.out.println("Columna Name" + colName );
								System.out.println("Columna DataType" + colDatatype );
								if(colDatatype.toUpperCase().equals(DECIMALTYPE)){
									String srcColvalue = lineValues[1];
									String trgColvale = lineValues[2];
									try{	
										BigDecimal bg1 = new BigDecimal(srcColvalue);
										BigDecimal bg2 = new BigDecimal(trgColvale);	
										int res;
										res = bg1.compareTo(bg2);	        
										if(res==0){
											continue;
										}else{
											bw.write(line);
											bw.newLine();
										}
									}catch(NumberFormatException nef){
										continue;
									}
								}else if(colDatatype.toUpperCase().equals(DATETYPE)|| colName.toLowerCase().contains(DATE_COL_REFERENCE) || 
										colName.toLowerCase().contains(DATE_COL_REFERENCE_1)){
									String srcColvalue = lineValues[1];
									String trgColvale = lineValues[2];
									trgColvale = trgColvale.replace(HYPHEN, "");
									srcColvalue = srcColvalue.replace(HYPHEN, "");
									if(!trgColvale.equals(srcColvalue)){
										bw.write(line);
										bw.newLine();
									}
								}
								else{
									bw.write(line);
									bw.newLine();
								}
								diffReportingColLimit.put(colName, currentValue+1);		
								//end part for updating if it is  first time 
							}else if(currentValue>=limitCount){
								break LIMITLoop;
							}
						}
					}
				}
			br.close(); 
			bw.close();
			//fs.delete(strinpuptFileName,true);
		} catch (ArrayIndexOutOfBoundsException  e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured while splitting the record line: " + line +e1.getMessage());
			log.error(exTrace);
		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while merging the diff files"+e.getMessage());
			log.error(exTrace);
		}
	}

	private static void mergeTartetexistcols(Path strinpuptFileName, Path outputFileName ){
		String line=null;
		//Path outputfile = new Path(outputFileName);	
		//	System.out.println("inputfilName======="+ strinpuptFileName);
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			if(fs.exists(outputFileName)){
				//System.out.println("exist " + outputFileName.toString());
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured while merging the diff files"+e1.getMessage());
			log.error(exTrace);
		}
		try {
			fs = FileSystem.get(new Configuration());
			FSDataInputStream is = fs.open(strinpuptFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.append(outputFileName)));
			int currentValue=0;
			LIMITLoop:
				while ((line = br.readLine()) != null){
					line = br.readLine();
					if(line !=null){
						String[] lineValues = line.split(COMMA);
						String colName = lineValues[0];
						//adding if it is  first time 
						if(targetReportingColLimit.get(colName) == null){
							targetReportingColLimit.put(colName, 0);	
							bw.write(line);
							bw.newLine();
							// end part for adding first time 
						}else{
							//updating if it is  first time 
							currentValue = targetReportingColLimit.get(colName);
							if(currentValue<limitCount){
								targetReportingColLimit.put(colName, currentValue +1);		
								bw.write(line);
								bw.newLine();							
							}else if(currentValue>=limitCount) {
								break LIMITLoop;
							}
						}
					}
				}
			br.close(); 
			bw.close();
			//fs.delete(strinpuptFileName,true);
		} catch (ArrayIndexOutOfBoundsException  e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured @mergeTartetexistcols while splitting the record line: " + line +e1.getMessage());
			log.error(exTrace);
		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured @mergeTartetexistcols while merging the diff files"+e.getMessage());
			log.error(exTrace);
		}
	}


	private static void mergeSourceexistcols(Path strinpuptFileName, Path outputFileName ){

		String line=null;
		//Path outputfile = new Path(outputFileName);			
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			if(fs.exists(outputFileName)){
				//System.out.println("exist " + outputFileName.toString());
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured while merging the diff files"+e1.getMessage());
			log.error(exTrace);
		}
		try {
			fs = FileSystem.get(new Configuration());
			FSDataInputStream is = fs.open(strinpuptFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.append(outputFileName)));
			int currentValue=0;
			LIMITLoop:
				while ((line = br.readLine()) != null){
					line = br.readLine();
					if(line !=null){
						String[] lineValues = line.split(COMMA);
						String colName = lineValues[0];
						//adding if it is  first time 
						if(sourceReportingColLimit.get(colName) == null){
							sourceReportingColLimit.put(colName, 0);	
							bw.write(line);
							bw.newLine();
							// end part for adding first time 
						}else{
							//updating if it is  first time 
							currentValue = sourceReportingColLimit.get(colName);
							if(currentValue<limitCount){
								sourceReportingColLimit.put(colName, currentValue +1);		
								bw.write(line);
								bw.newLine();							
							}else if(currentValue>=limitCount){
								//break LIMITLoop;
							}
						}
					}
				}
			br.close(); 
			bw.close();
			//fs.delete(strinpuptFileName,true);
		} catch (ArrayIndexOutOfBoundsException  e1) {
			// TODO Auto-generated catch block
			String exTrace = DWUtil.getStackTraceAsString(e1);
			log.error("Error occured @mergeSourceexistcols while splitting the record line: " + line +e1.getMessage());
			log.error(exTrace);
		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured @mergeSourceexistcols while merging the diff files"+e.getMessage());
			log.error(exTrace);
		}
	}
	
	public static Properties getColDataTypes(String colName_Datatype ){
		Properties colDataTypes = new Properties();
		try{
			String[] mappingColl = colName_Datatype.split(COMMA);
			for(String colValue: mappingColl){
				String[] nameDatatype = colValue.split(SINGLE_COLON);
				String colName = nameDatatype[0];
				String dataType = nameDatatype[1];
				colDataTypes.setProperty(colName, dataType);
			}
		}catch(Exception e){
			
		}
		 return colDataTypes;
	}
 
}

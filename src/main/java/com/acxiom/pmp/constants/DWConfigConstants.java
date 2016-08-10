package com.acxiom.pmp.constants;

import java.io.File;

public interface DWConfigConstants {
	// Property file constants
	String DWVALIDATION_START_DATAE = "dwvalidation.start.date";
	String DWVALIDATION_END_DATAE =  "dwvalidation.end.date";
	String DWVALIDATION_SOURCE_TABLES_DATA_LOCATON="dwvalidation.source.tables.data.location";	
	String DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE="";
	String DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE= "dwvalidation.target.hive.table.tocompare";
	String DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON="dwvalidation.target.dw.table.data.location";
	String DWVALIDATION_RESULT_LOCATION="dwvalidation.result.location";
	String DWVALIDATION_TARGET_HEADER="dwvalidation.target.header";
	String DWVALIDATION_SOURCE_HEADERS = "dwvalidation.headers";
	String DWVALIDATION_ROW_KEY="empty";
	String DWVALIDATION_COL_SAMPLING_COUNT="dwvalidation.col.sampling.cout";
	String DWVALIDATION_SOURCE_EXCLUDED_TABLES="dwvalidation.source.excluded.tables";
	String DWVALIDATION_SOURCE_QUOTES_TABLES = "empty";
	String DWVALIDATION_DETAILED_RESULT = "dwvalidation.detailed.result";
	// Utility Constants 
	String DATE_COL_INDEXS="0";
	String TABLE_NAME_SPLITTER_FROM_FNAME="_1TIME_";
	String COMMA = ",";
	String FSEP = File.separator;
	String LSEP = System.getProperty("line.separator");
	String APPENDER = "\u00FF\u00FF";
	String SEMICOLON = ";";
	String TAB = "\t";
	String TILD = "~";
	String EQUALS = "=";
	String DOT = "\\.";
	String SINGLEDOT=".";
	String UNDERSCORE = "_";
	String FOPBRACKET = "{";
	String FCLBRACKET = "}";
	String PIPE = "|";
	String ADDITIONAL_NEWLINE= ",newline";
	String COMPARISION_FULL="FULL";
	String OPBRACKET = "[";
	String CLBRACKET = "]";
	String DIFFERED_COLS="differed_cols";
	String MERGED_DIFFERED_COLS="merged_differed_cols_result.tsv";
	String MATCHED_COLS="matched_cols";
	String MATCHED_RECORDS="matched_records";
	String EXISTS_ONLY_IN_SOURCE_COLS="exists_only_in_source_cols";
	String ROWS_EXISTS_ONLY_IN_SOURCE_COLS="rows_exists_only_in_source_cols";
	String MERGED_EXISTS_ONLY_IN_SOURCE_COLS="merged_exists_only_in_source_cols.tsv";
	String EXISTS_ONLY_IN_TARGET_COLS="exists_only_in_target_cols";
	String MERGED_EXISTS_ONLY_IN_TARGET_COLS="merged_exists_only_in_target_cols.tsv";
	String NOT_UPDATED_TO_NULL="not_updated_to_null";
	String UNMATCHED_DUPLICATE_RECORDS="unmatched_duplicate_records";
	String SAMPLING_FOLDER_NAME= "LIMITBY";
	String DATE_COL_REFERENCE= "_dt";
	String DATE_COL_REFERENCE_1="_date";
	String HYPHEN="-";
	String EMPTY = "";
	String QUOTES="\"";
	String SINGLE_COLON=":";
	String DATA_FOLDER_NAME="Data";
	String DMART_FOLDER_NAME="DATAMART";
	String Header_NAME= "Header";
	
	// conf file identifier  Constants 
	String COLMAPPINGFILE="dmart.column.datatypes.mapping.properties";

	//Mapper Constant
	String ORC_NULL= "\\N";
	String THORN="\u00FE";
	String SFTYPE="source";
	String TFTYPE="targert";
	
	//ResultsRuner constants
	String DECIMALTYPE = "DECIMAL";
	String DATETYPE ="DATE";
	
	//table specific property file properties
	String ROW_KEY_NAME=".row.key";
	String QUOTE_COLS_NAME=".quotes.cols";
	
	


}

package com.eb.bi.rs.frame2.service.dataload.file2redis;

import java.util.ArrayList;
import java.util.List;

public class LineParser {
	
	
	/**
	 * get key using key pattern from input line.
	 * @param line  delimited by | 
	 * @param key   delimited by $
	 * @return
	 */
	public static String getKey(String line, String key) {

		String[] fields = line.split("\\|", -1);
		String resultKey = key;
		
		for (int i = 0; i < fields.length; i++) {	
			String replacer = "$(" + i + ")";
			resultKey = resultKey.replace(replacer, fields[i]);
		}		
		return resultKey;
	}
	
	 
	public static String getField(String line, String field) {

		String[] fields = line.split("\\|", -1);
		String resultField = field;
		
		for (int i = 0; i < fields.length; i++) {		
			String replacer = "$(" + i + ")";
			resultField = resultField.replace(replacer, fields[i]);
		}
		
		return resultField;
	}

	
	public static String getValue(String line, String value) 	{

		String[] fields = line.split("\\|",-1);
		String resultValue = value;		
		for (int i = 0; i < fields.length; i++)		{
			String replacer = "$(" + i + ")";
			resultValue = resultValue.replace(replacer, fields[i]);
		}		
		return resultValue;
	}
	
	
	public static ArrayList<String> getMField(String field) {	//这个line没有
		ArrayList<String > resultField = new ArrayList<String>();
		String[] fields = field.split(",");	
		for (String fi : fields) {
			resultField.add(fi);		
		}		
		return resultField;
	}

	
	public static ArrayList<String> getMValue(String line, String value) {		
		
		String[] values = value.split(",");	
		String[] fields = line.split("\\|",-1);		
		ArrayList<String > resultValue = new ArrayList<String>();		
		for (String val : values) {	
			for (int i = 0; i < fields.length; i++)		{
				String replacer = "$(" + i + ")";
				val = val.replace(replacer, fields[i]);
			}
			resultValue.add(val);			
		}
		
		return resultValue;
	}
	
	
	public static List<String> getFields(String line, int grpBegIdx, int grpLen, ArrayList<Integer> fieldLocInGrp, String delimiter)  {
		
		
		String[] fields = line.split("\\|",-1);
		List<String> values = new ArrayList<String>();
		
		for (int i = grpBegIdx; i < fields.length; i += grpLen) {	
			if (grpLen == 1) {
				values.add(fields[i]);
			}else {
				StringBuffer sb = new StringBuffer();
				for (int j = i; j < i + grpLen; j++) {					
					if (fieldLocInGrp.contains(j - i)) {
						sb.append(fields[j] + delimiter);//分隔符可配置
					}
				}
				
				sb.deleteCharAt(sb.length()-1);
				values.add(sb.toString());
			}			
		}
		
		return values;
		
	}
	
	
	/*有几个group就有几个value*/
	public static List<String> getValues(String line, int grpBegIdx, int grpLen, ArrayList<Integer> valLocInGrp, String delimiter) 	{

		String[] fields = line.split("\\|",-1);
		List<String> values = new ArrayList<String>();
		
		for (int i = grpBegIdx; i < fields.length; i += grpLen) {	
			if (grpLen == 1) {
				values.add(fields[i]);
			}else {
				StringBuffer sb = new StringBuffer();
				for (int j = i; j < i + grpLen; j++) {					
					if (valLocInGrp.contains(j - i)) {
						sb.append(fields[j] + delimiter);//分隔符可配置
					}
				}				
				sb.deleteCharAt(sb.length()-1);
				values.add(sb.toString());
			}			
		}
		
		return values;
	}

}

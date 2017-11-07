package com.eb.bi.rs.andedu.inforec.get_idf_value;

public class FilterChar {
	public static String clearNotChinese(String buff){
     	String tmpString =buff.replaceAll("(?i)[^a-zA-Z0-9\u4E00-\u9FA5]", "");//去掉所有中英文符号
    	char[] carr = tmpString.toCharArray();
    	for(int i = 0; i<tmpString.length();i++){
    		if(carr[i] < 0xFF){
    			carr[i] = ' ' ;//过滤掉非汉字内容
    		}
    	}
    	return String.copyValueOf(carr).trim();
    }
}

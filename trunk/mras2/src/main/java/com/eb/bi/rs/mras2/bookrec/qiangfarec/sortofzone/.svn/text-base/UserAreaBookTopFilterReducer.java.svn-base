package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserAreaBookTopFilterReducer extends Reducer<Text, Text, NullWritable, Text> {

	private String fieldDelimiter;
	private String fieldDelimiter1;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		fieldDelimiter1 = conf.get("field.delimiter.1", ",");
	}
	
	// key  msisdn_专区ID ;  values bookid1 ,bookid2 ,book3,...
	// key  msisdn_专区id ;  values bookid图书，bookid图书
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String output = key.toString() + ";";
		
		String[] topNBookInfo = null;
		String resultBookID = ""; 
		
		for (Text pair : values) {
			
			String[] fields = pair.toString().split(fieldDelimiter);
			
			if( fields[0].equals("topN") ) { // 用户专区top n 图书
				topNBookInfo = fields;
			}
			
			if( fields[0].equals("result") ) { // 用户专区排序结果图书
				resultBookID = fields[1].toString();
			}
		}
		
		String result = resultBookID;
		if( topNBookInfo != null){
			for(int i = 1; i < topNBookInfo.length; i++){
				output += topNBookInfo[i] + "|"; // 将top N数据放在最前面
				
				int index;
				String temp = "";
				if (( index = result.indexOf(topNBookInfo[i])) != -1){
					temp = result.substring(0, index);
					if(index + topNBookInfo[i].length() == result.length()){
						temp += result.substring(index + topNBookInfo[i].length(), result.length());
					} else {
						temp += result.substring(index + topNBookInfo[i].length() + 1, result.length());
					}
				}else{
					temp = result;
				}
				
				result = temp;
				
			}
			String s1 = result.replace(',','|');
			output += s1;
		
			// msisdn_专区ID;bookid1|bookid2|...
			if(resultBookID.length() != 0){
				context.write(NullWritable.get(), new Text(output));
			}
		}
	}
}

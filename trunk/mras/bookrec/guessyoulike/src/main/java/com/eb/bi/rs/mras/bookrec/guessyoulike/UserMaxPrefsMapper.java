package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMaxPrefsMapper extends Mapper<NullWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {
		//用户|待推荐图书|源图书集|相似度|属性偏好，eg。u1|a1|1:b1,b3|0.1,0.2,0.3|1.0,4.0,9.0			
		String[] fields = value.toString().split("\\|",-1);
		if(fields.length == 5) {			
			outKey.set(fields[0]);
			outValue.set(fields[3] + "|" + fields[4]);			
		}else if (fields.length == 4) {
			outKey.set(fields[0]);
			outValue.set(fields[3]);
		}
		context.write(outKey, outValue);
	}	

}

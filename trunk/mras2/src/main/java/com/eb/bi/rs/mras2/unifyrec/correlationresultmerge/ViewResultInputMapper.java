package com.eb.bi.rs.mras2.unifyrec.correlationresultmerge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ViewResultInputMapper extends Mapper<LongWritable, Text, Text, Text> {
	//读取浏览还浏览关联推荐的结果，输入数据格式为：bookA_id|book_id|similarity|book_id|similarity，输出数据为<bookA_id, book_id|similarity|book_id|similarity>
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] items = value.toString().split("\\|", -1);
		assert items.length == 10;		
		StringBuffer sb = new StringBuffer();
		if(Integer.parseInt(items[9]) == 1) {
			sb.append(items[1] + "|" + items[5]);
			context.write(new Text(items[0]), new Text(sb.toString()));
		}
	}
}

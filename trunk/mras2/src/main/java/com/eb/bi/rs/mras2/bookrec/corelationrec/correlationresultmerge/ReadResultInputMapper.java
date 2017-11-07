package com.eb.bi.rs.mras2.bookrec.corelationrec.correlationresultmerge;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadResultInputMapper extends Mapper<LongWritable, Text, Text, Text> {
	//读取阅读还阅读关联推荐的结果，输入数据格式为：bookA_id|book_id|similarity|book_id|similarity，输出数据为<bookA_id, book_id|similarity|book_id|similarity>
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

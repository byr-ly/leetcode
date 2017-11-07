package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinClassMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		/*book_id动漫ID|author_id作者ID|class_id分类ID|charge_type计费类型|点击量*/
		String[] fields = value.toString().split("\\|", -1);
		if(fields.length >= 5){
			context.write(new TextPair(fields[0], "0"), new TextPair(fields[1]+"|"+fields[2]+"|"+fields[3]+"|"+fields[4],"0"));							
		}
	}
	

}

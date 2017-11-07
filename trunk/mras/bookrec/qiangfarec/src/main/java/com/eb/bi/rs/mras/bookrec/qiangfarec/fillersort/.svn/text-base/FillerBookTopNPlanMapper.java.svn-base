package com.eb.bi.rs.mras.bookrec.qiangfarec.fillersort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FillerBookTopNPlanMapper extends Mapper<Object, Text, Text, Text>{
	private String fieldDelimiter1;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter1 = conf.get("field.delimiter.1", ";");		
	}
	
	// 序号_zoneId;bid|bid
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(fieldDelimiter1);
		
		if ( fields[0] != null && fields[1] != null ) {
			context.write(new Text(fields[0]), new Text(fields[1]));
		}
	}
}

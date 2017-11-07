package com.eb.bi.rs.mras2.bookrec.channelrec.filler;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ChannelRecFillerMapper extends Mapper<Object, Text, Text, Text>  {

	@Override
	protected void map(Object key, Text value,Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] fields = value.toString().split("\\|");//channel_type|bookid|classid|book_stype
		
		if (fields.length == 4 && "1".equals(fields[3])) {	
			String channel_type = fields[0];
			String bookId = fields[1];
			context.write(new Text(channel_type), new Text(bookId));
		}		
	}
}

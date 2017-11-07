package com.eb.bi.rs.mras2.bookrec.qiangfarec.filler;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SelectHighlyRecommendBooksFillerMapper extends Mapper<Object, Text, Text, Text>  {

	@Override
	protected void map(Object key, Text value,Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		String[] fields = value.toString().split("\\|");//专区id | bookid |bu_type事业部| if_rec是否强推| if_on是否在当前专区|ontime 上专区时间
		
		if (fields.length == 6) {	
			String zoneId = fields[0];
			String bookId = fields[1];
			String if_rec = fields[3];
			String if_on = fields[4];
			//如果用户为深度、中度或强偏执用户
			if ("1".equals(if_rec) && "1".equals(if_on) && !StringUtils.isBlank(bookId)) {
				context.write(new Text(zoneId), new Text(bookId));
			}			
		}		
	}
}

package com.eb.bi.rs.mras.voicebookrec.lastpage;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SerialBookRecMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		/*图书ID|系列ID|系列顺序|栏目ID*/
		String[] fields = value.toString().split("\\|",-1);
		
		String serialId = fields[1];
		/*当SERIES_ID不为空且不等于’-1’时，若SERIES_ID值一致则属于同系列的图书*/
		if(!"".equals(serialId) && !"-1".equals(serialId)) {
			context.write(new Text(serialId), value);
		}		
	}
}

package com.eb.bi.rs.opus.itemcf.similarity.mark;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ynn 
 * 
 */
public class DownLoadMatrixMappper extends Mapper<LongWritable, Text, Text, Text> {

	/*
	 * 格式：IMEI用户ID|动漫ID|下载次数|lastday最后一次操作时间
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if(fields.length >= 3){
			// key : 用户ID|动漫ID	  value : 0|次数 ，0表示下载行为
			if(!"".equals(fields[0]) && !"".equals(fields[1])){
				context.write(new Text(fields[0] + "|" + fields[1]), new Text("0|" + fields[2]));
			}
		}
	}
}

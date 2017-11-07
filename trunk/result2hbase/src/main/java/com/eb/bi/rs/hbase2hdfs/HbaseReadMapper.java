package com.eb.bi.rs.hbase2hdfs;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;

import javax.ws.rs.core.NewCookie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class HbaseReadMapper extends TableMapper<Text, NullWritable>{
	private String[] columns = null;
	private String split = null;
	private String express = null;
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		split = conf.get("conf.export.split");
		express = conf.get("conf.export.express");
		columns = express.split(split, -1);
		split = split.replace("\\", "");
	}
	
	public void map(ImmutableBytesWritable key, Result value, Context context) {
		String result = new String();
		try {
			for(String column : columns) {
				if(column.equals("rowkey")) {
					result += Text.decode(key.get()) + split;
					continue;
				}
				String cf[] = column.split(":", -1);
				if (cf.length == 2 && value.containsColumn(Bytes.toBytes(cf[0]), Bytes.toBytes(cf[1]))) {
					result += Text.decode(value.getValue(Bytes.toBytes(cf[0]), Bytes.toBytes(cf[1]))) + split;
				} else {
					return;
				}
			}
		} catch (CharacterCodingException e) {
			e.printStackTrace();
		}
		result = result.substring(0, result.length()-split.length());
		context.write(new Text(result), NullWritable.get());
		// 将结果写入文件
//		try {
//			context.write(new Text(result), NullWritable.get());
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
	}

}

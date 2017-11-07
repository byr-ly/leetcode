package com.eb.bi.rs.opus.hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImportDataProMapper extends Mapper<Object, Text, Text, Text> {
	
	private String dataSplit = "";
	private String rowkeyIndexs = "";
	private String rowkeySplit = "";
	private int[] rowkeyIndexList = null;
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		dataSplit = conf.get("conf.data.split");
		rowkeyIndexs = conf.get("conf.rowkey.indexs");
		rowkeySplit = conf.get("conf.rowkey.split");
		String tmpVector[] = rowkeyIndexs.split(",", -1);
		rowkeyIndexList = new int[tmpVector.length];
		for (int i = 0; i<tmpVector.length; i++) {
			rowkeyIndexList[i] = Integer.parseInt(tmpVector[i]);
		}
	}
	
	@Override
	protected void map(Object o, Text value, Context context) throws IOException, InterruptedException {
		String dataVector[] = value.toString().split(dataSplit);
		String rowkey = "";
		for (int index : rowkeyIndexList) {
			rowkey += dataVector[index] + rowkeySplit;
		}
		context.write(new Text(rowkey), new Text(value));
	}

}

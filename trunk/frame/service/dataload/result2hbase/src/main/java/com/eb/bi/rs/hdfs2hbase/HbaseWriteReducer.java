package com.eb.bi.rs.hdfs2hbase;

import java.io.IOException;

import org.apache.commons.net.nntp.NewGroupsOrNewsQuery;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class HbaseWriteReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
	private String[] columns = null;
	private String split = "";
	private int splitNum = -1;
	private String express = "";
	private String rowkeySplit = "";
	private String rowkeyExpress = "";
	private int[] rowkeyIndexs = null;
	private int rowkeyIndex = -1;
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		split = conf.get("conf.import.split");
		try {
			splitNum = Integer.parseInt(conf.get("conf.import.splitnum"));
		} catch (NumberFormatException e) {
			splitNum = -1;
		}
		
		express = conf.get("conf.import.express");
		columns = express.split(split, splitNum);
		// 为适配旧版本的代码，从columns中获取rowkey位置
		for (int i=0; i<columns.length; i++) {
			if(columns[i].equals("rowkey")) {
				rowkeyIndex = i;
			}
		}
		// 得到rowkey的各个字段的index
		rowkeySplit = conf.get("conf.rowkey.split");
		rowkeyExpress = conf.get("conf.rowkey.express");
		if (rowkeySplit == null || rowkeyExpress == null) {
			rowkeyIndexs = new int[1];
			rowkeyIndexs[0] = rowkeyIndex;
			rowkeySplit = "_";
		} else {
			String[] rowkeyIndexStr = rowkeyExpress.split(rowkeySplit, -1);
			try {
				rowkeyIndexs = new int[rowkeyIndexStr.length];
				int i = 0;
				for (String index : rowkeyIndexStr) {
					rowkeyIndexs[i] = Integer.parseInt(index);
					i++;
				}
			} catch (NumberFormatException e) {
				throw new InterruptedException("config conf.rowkey.express error");
			}
		}
		// 判断配置参数异常
		if (split.isEmpty() || columns.length == 0 || rowkeyIndexs.length == 0) {
			throw new InterruptedException("config file param error");
		}
	};
	
	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		String[] tokens = key.toString().split(split, splitNum);
		if (tokens.length != columns.length) {
			throw new InterruptedException("Data param num and config prarm not equal");
		}
		// 組裝rowkey
		String rowKeyStr = "";
		for (int index : rowkeyIndexs) {
			rowKeyStr += tokens[index] + rowkeySplit;
		}
		rowKeyStr = rowKeyStr.substring(0, rowKeyStr.length()-rowkeySplit.length());
		// 插入hbase
		byte[] bRowKey = Bytes.toBytes(rowKeyStr);
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
		Put p = new Put(bRowKey);
		for (int i=0; i<tokens.length; i++) {
			if (columns[i].equals("rowkey")) continue;
			String[] familyAndColumn = columns[i].split(":", -1);
			if (familyAndColumn.length < 2) {
				throw new InterruptedException("cf and column format error");
			}
			p.add(Bytes.toBytes(familyAndColumn[0]), Bytes.toBytes(familyAndColumn[1]), Bytes.toBytes(tokens[i]));
		}
		context.write(rowKey, p);
	}
}


package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class FamousRecomReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();

		for (Text value : values) {
			sb.append(value.toString() + "|");
		}

		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		System.out.println("SB: " + sb);

		byte[] bRowKey = Bytes.toBytes(key.toString());
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
		Put p = new Put(bRowKey);
		p.add(Result2HbaseTable.BCF, Result2HbaseTable.BCOL_FAMOUS, Bytes.toBytes(sb.toString()));

		context.write(rowKey, p);
	}
}

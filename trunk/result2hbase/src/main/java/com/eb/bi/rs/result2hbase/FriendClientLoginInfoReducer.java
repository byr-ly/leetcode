package com.eb.bi.rs.result2hbase;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class FriendClientLoginInfoReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] fields;
		byte[] bRowKey;
		for (Text value : values) {
			fields = value.toString().split("\\|", -1);
			bRowKey = Bytes.toBytes(key.toString());
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
			Put p = new Put(bRowKey);
			p.add(Bytes.toBytes("cf"), Bytes.toBytes("phone"), Bytes.toBytes(fields[0]));
			p.add(Bytes.toBytes("cf"), Bytes.toBytes("lasttime"), Bytes.toBytes(fields[1]));
			p.add(Bytes.toBytes("cf"), Bytes.toBytes("firsttime"), Bytes.toBytes(fields[2]));
			context.write(rowKey, p);
		}
	}
}


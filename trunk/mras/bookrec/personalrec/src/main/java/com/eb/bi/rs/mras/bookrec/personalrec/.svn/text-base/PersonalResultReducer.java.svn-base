package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;


public class PersonalResultReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();
		for (Text value : values) {
			sb.append(value.toString() + ",");
		}

		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}

		byte[] bRowKey = Bytes.toBytes(key.toString());
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
		Put p = new Put(bRowKey);
		p.add(PersonalResultTable.BCF, PersonalResultTable.BCOL, Bytes.toBytes(sb.toString()));

		context.write(rowKey, p);
	}
}

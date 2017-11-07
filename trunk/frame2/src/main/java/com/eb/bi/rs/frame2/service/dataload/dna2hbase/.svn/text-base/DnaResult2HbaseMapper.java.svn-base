package com.eb.bi.rs.frame2.service.dataload.dna2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//在map阶段就直接进行put操作，省掉了map到reduce之间的网络IO，节省了程序的运行时间。
//这个程序会将每一行的第一个字段取出来作为rowkey，然后再把这一行的全量数据做为值，最
//后把这条数据插入hbase。
public class DnaResult2HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private String split;

	public void map(LongWritable o, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(split, -1);
		byte[] bRowKey = items[0].getBytes();
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
		Put p = new Put(bRowKey);
		p.add(DnaResult2HbaseTable.BCF, DnaResult2HbaseTable.BCOL_RESULT, Bytes.toBytes(value.toString()));
		context.write(rowKey, p);
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		split = conf.get("conf.text.split");
	}
}

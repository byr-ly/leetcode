package com.eb.bi.rs.frame2.service.dataload.dna2hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DnaChaptCnt2HbaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString() + ",");
        }
        if (sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);
        byte[] bRowKey = Bytes.toBytes(key.toString());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
        Put p = new Put(bRowKey);
        p.add(DnaResult2HbaseTable.BCF, DnaResult2HbaseTable.BCOL_CHAPTCNT, Bytes.toBytes(sb.toString()));
        context.write(rowKey, p);
    }
}

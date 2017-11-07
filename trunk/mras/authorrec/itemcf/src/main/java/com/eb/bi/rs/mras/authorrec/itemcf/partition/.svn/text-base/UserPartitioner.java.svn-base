package com.eb.bi.rs.mras.authorrec.itemcf.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class UserPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // TODO Auto-generated method stub
        String keyStr = key.toString();
        String sub = keyStr.substring(8);
        int tail = Integer.parseInt(sub);
        int mo = tail % numPartitions;
        return mo;
    }

}

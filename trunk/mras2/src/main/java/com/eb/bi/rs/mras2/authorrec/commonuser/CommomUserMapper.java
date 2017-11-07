package com.eb.bi.rs.mras2.authorrec.commonuser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by liyang on 2016/3/8.
 */
public class CommomUserMapper extends Mapper<LongWritable,Text,Text,Text>{
    Logger log = Logger.getLogger(CommomUserMapper.class);
    @Override
    public void map(LongWritable key,Text value,Context context)
        throws IOException,InterruptedException{
        context.write(new Text(" "),new Text(value));
    }
}

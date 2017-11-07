package com.eb.bi.rs.mras2.consonance.lubricationdata;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by houmaozheng on 2017/6/23.
 */
public class LubricationDataDriver extends BaseDriver{

    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Logger log = Logger.getLogger("SortBookEditPointsDriver");
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration(getConf());

        String value;
        if ((value = properties.getProperty("field.delimiter"))!= null) {
            conf.set("field.delimiter", value);
        }
        if ((value = properties.getProperty("action.num")) != null) {
            log.info(value);
            conf.set("action.num", value);
        }
        if ((value = properties.getProperty("record.day")) != null) {
            log.info(value);
            conf.set("record.day", value);
        }
        String reduceNum =  properties.getProperty("mapred.reduce.tasks");
        if(reduceNum != null){
            log.info(reduceNum);
            conf.set("mapred.reduce.tasks", reduceNum);
        }

        Job job = new Job(conf,getClass().getSimpleName());
        job.setJarByClass(getClass());

        job.setReducerClass(LubricationDataReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //输入输出相关配置
        String InputPath = properties.getProperty("hdfs.input.path");
        if (InputPath == null) {
            throw new RuntimeException("input path is essential");
        }
        log.info(InputPath);
        MultipleInputs.addInputPath(job, new Path(InputPath), TextInputFormat.class, LubricationDataMapper.class);

        String OutputPath = properties.getProperty("hdfs.output.path");
        if(OutputPath == null){
            throw new RuntimeException("output path is essential");
        }
        check(OutputPath,conf);
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if( job.waitForCompletion(true)){
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 0;
        }
        else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
    }

    public void check(String path, Configuration conf)
    {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

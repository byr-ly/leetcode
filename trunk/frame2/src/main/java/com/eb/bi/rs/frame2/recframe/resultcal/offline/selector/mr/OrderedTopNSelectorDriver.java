package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.StringDoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderedTopNSelectorDriver extends BaseDriver {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        String value;
        if ((value = properties.getProperty("field.delimiter")) != null) {
            conf.set("field.delimiter", value);
        }
        if ((value = properties.getProperty("key.field.indexes")) != null) {
            conf.set("key.field.indexes", value);
        }
        if ((value = properties.getProperty("order.by.field.index")) != null) {
            conf.setInt("order.by.field.index", Integer.parseInt(value));
        }
        if ((value = properties.getProperty("select.number")) != null) {
            conf.setInt("select.number", Integer.parseInt(value));
        }

        //输入输出相关配置
        String inputPaths = properties.getProperty("input.path");
        if (inputPaths == null) {
            throw new RuntimeException("input path is essential");
        }
        String outputPath = properties.getProperty("output.path");
        if (outputPath == null) {
            throw new RuntimeException("output path is essential");
        }
        //mr配置相关
        if ((value = properties.getProperty("mapred.reduce.tasks")) != null) {
            conf.set("mapred.reduce.tasks", value);
        }

        if ((value = properties.getProperty("mapreduce.inputformat.class")) != null) {
            conf.setClass("mapreduce.inputformat.class", Class.forName(value), InputFormat.class);
        }

        if ((value = properties.getProperty("mapreduce.outputformat.class")) != null) {
            conf.setClass("mapreduce.outputformat.class", Class.forName(value), OutputFormat.class);
        }

        if ((value = properties.getProperty("mapred.output.compress")) != null) {
            conf.setBoolean("mapred.output.compress", Boolean.parseBoolean(value));
        }

        if ((value = properties.getProperty("mapred.output.compression.codec")) != null) {
            conf.setClass("mapred.output.compression.codec", Class.forName(value), CompressionCodec.class);
        }

        if ((value = properties.getProperty("mapred.output.compression.type")) != null) {
            conf.set("mapred.output.compression.type", value);
        }

        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(getClass());

        job.setMapperClass(OrderedTopNSelectorMapper.class);
        job.setCombinerClass(OrderedTopNSelectorCombiner.class);
        job.setReducerClass(OrderedTopNSelectorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringDoublePair.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPaths);
        check(outputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void check(String path, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

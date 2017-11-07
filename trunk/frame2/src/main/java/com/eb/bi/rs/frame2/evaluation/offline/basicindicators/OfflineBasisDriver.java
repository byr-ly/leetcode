package com.eb.bi.rs.frame2.evaluation.offline.basicindicators;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by houmaozheng on 2016/12/22.
 */
public class OfflineBasisDriver extends BaseDriver {

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Logger log = Logger.getLogger("RecAvgOLMtxDriver");
        long start = System.currentTimeMillis();
        Job job = null;
        Configuration conf = new Configuration(getConf());

        String value;
        if ((value = properties.getProperty("field.delimiter")) != null) {
            conf.set("field.delimiter", value);
        }

        String reduceNum = properties.getProperty("mapred.reduce.tasks");
        if (reduceNum != null) {
            conf.set("mapred.reduce.tasks", reduceNum);
        }

        job = Job.getInstance(conf);
        job.setJarByClass(OfflineCountDriver.class);

        //输入输出相关配置
        String dataInputPath = properties.getProperty("hdfs.data.input.path");
        if (dataInputPath == null) {
            throw new RuntimeException("hdfs data input path is essential");
        }
        FileInputFormat.setInputPaths(job, dataInputPath);

        String dataOutputPath = properties.getProperty("hdfs.data.output.path");
        if (dataOutputPath == null) {
            throw new RuntimeException("hdfs data output path is essential");
        }
        check(dataOutputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(dataOutputPath));

        job.setNumReduceTasks(0);
        job.setMapperClass(OfflineBasisMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 0;
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
    }

    private void check(String path, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


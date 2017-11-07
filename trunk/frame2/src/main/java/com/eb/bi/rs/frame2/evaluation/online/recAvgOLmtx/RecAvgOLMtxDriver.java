package com.eb.bi.rs.frame2.evaluation.online.recAvgOLmtx;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 16/12/5.
 * 整体准确率计算
 */
public class RecAvgOLMtxDriver extends BaseDriver {


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
        job.setJarByClass(RecAvgOLMtxDriver.class);

        //输入输出相关配置
        String dataInputPath = properties.getProperty("hdfs.data.input.path");
        if (dataInputPath == null) {
            throw new RuntimeException("hdfs data Bool If Score Path input path is essential");
        }
        MultipleInputs.addInputPath(job, new Path(dataInputPath), TextInputFormat.class, RecAvgOLMtxMapper.class);

        String dataOutputPath = properties.getProperty("hdfs.data.output.path");
        if (dataOutputPath == null) {
            throw new RuntimeException("hdfs data output path is essential");
        }
        dataOutputPath += "/" + getNowdate();//以日期为目录
        check(dataOutputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(dataOutputPath));

        job.setCombinerClass(RecAvgOLMtxCombiner.class);
        job.setNumReduceTasks(Integer.parseInt(reduceNum));
        job.setReducerClass(RecAvgOLMtxReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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

    public void check(String path, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}

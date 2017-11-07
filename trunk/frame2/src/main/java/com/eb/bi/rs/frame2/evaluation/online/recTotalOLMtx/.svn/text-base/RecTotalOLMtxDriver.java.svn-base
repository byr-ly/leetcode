package com.eb.bi.rs.frame2.evaluation.online.recTotalOLMtx;

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
public class RecTotalOLMtxDriver extends BaseDriver {


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
        
        String fieldDigit = properties.getProperty("field.digit");
        if (fieldDigit != null) {
            conf.set("field.digit", fieldDigit);
        }
        
        String fieldNum = properties.getProperty("field.num");
        if (fieldNum != null) {
            conf.set("field.num", fieldNum);
        }
        
        for (int i = 1; i<=Integer.parseInt(fieldNum); i++)
        {
        	String confSet = "field.number." + i;
        	String fieldNumber = properties.getProperty(confSet);
            if (fieldNumber != null) {
                conf.set(confSet, fieldNumber);
            }
        }

        job = Job.getInstance(conf);
        job.setJarByClass(RecTotalOLMtxDriver.class);

        // 输入输出相关配置
        // 用户推荐位点击图书数（去重）表
        String dataNucInputPath = properties.getProperty("hdfs.data.nuc.input.path");
        if (dataNucInputPath == null) {
            throw new RuntimeException("hdfs data nuc input path is essential");
        }
        MultipleInputs.addInputPath(job, new Path(dataNucInputPath), TextInputFormat.class, RecNucMapper.class);

        // 用户推荐位订购图书数（去重）表
        String dataNuoInputPath = properties.getProperty("hdfs.data.nuo.input.path");
        if (dataNuoInputPath == null) {
            throw new RuntimeException("hdfs data nuo input path is essential");
        }
        MultipleInputs.addInputPath(job, new Path(dataNuoInputPath), TextInputFormat.class, RecNuoMapper.class);

        // 用户推荐位图书数（去重）表
        String dataNurInputPath = properties.getProperty("hdfs.data.nur.input.path");
        if (dataNurInputPath == null) {
            throw new RuntimeException("hdfs data nur input path is essential");
        }
        MultipleInputs.addInputPath(job, new Path(dataNurInputPath), TextInputFormat.class, RecNurMapper.class);

        String dataOutputPath = properties.getProperty("hdfs.data.output.path");
        if (dataOutputPath == null) {
            throw new RuntimeException("hdfs data output path is essential");
        }
        dataOutputPath += "/" + getNowdate();//以日期为目录
        check(dataOutputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(dataOutputPath));

        job.setCombinerClass(RecTotalOLMtxCombiner.class);
        job.setNumReduceTasks(Integer.parseInt(reduceNum));
        job.setReducerClass(RecTotalOLMtxReducer.class);

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

    private void check(String path, Configuration conf) {
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

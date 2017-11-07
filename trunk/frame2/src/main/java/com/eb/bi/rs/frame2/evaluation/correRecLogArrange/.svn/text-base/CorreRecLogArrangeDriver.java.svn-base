package com.eb.bi.rs.frame2.evaluation.correRecLogArrange;

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
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by chenyuxiao on 17/08/28.
 * web MyTime日志
 * 输出：
 * 用户|推荐位|看到的图书ID1,看到的图书ID2... | 时间
 */
public class CorreRecLogArrangeDriver extends BaseDriver {


    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Logger log = Logger.getLogger("CorreRecLogArrangeDriver");
        long start = System.currentTimeMillis();
        Job job = null;
        Configuration conf = new Configuration(getConf());

        job = Job.getInstance(conf);
        job.setJarByClass(CorreRecLogArrangeDriver.class);

        String recType = properties.getProperty("rec.type");
        if (recType != null) {
            conf.set("rec.type", recType);
        }
        //输入输出相关配置
        String dataInputPath = properties.getProperty("hdfs.data.input.path");
        if (dataInputPath == null) {
            throw new RuntimeException("hdfs data input path is essential");
        }

        String dataOutputPath = properties.getProperty("hdfs.data.output.path");
        if (dataOutputPath == null) {
            throw new RuntimeException("hdfs data output path is essential");
        }
        check(dataOutputPath, conf);
        job.setMapperClass(CorreRecLogArrangeMapper.class);
        FileInputFormat.setInputPaths(job, new Path(dataInputPath+getNowDate()));
        FileOutputFormat.setOutputPath(job, new Path(dataOutputPath));

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

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
	public String getNowDate(){   
	    String temp_str="";   
	    Date dt = new Date(new Date().getTime()-24*60*60*1000);   
	    //HH表示24小时制    如果换成hh表示12小时制   
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");   
	    temp_str=sdf.format(dt);   
	    return temp_str;   
	}
}


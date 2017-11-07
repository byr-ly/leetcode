package com.eb.bi.rs.frame2.recframe.knowledgecal.usertagweight;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

public class UserLabelComputeDriver extends BaseDriver {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Logger log = Logger.getLogger("UserLabelComputeDriver");
        long start = System.currentTimeMillis();
        Job job = null;
        Configuration conf;
        conf = new Configuration(getConf());

        Properties app_conf = super.properties;
        //���ü���--------------------------------------------------
        String inputPath = app_conf.getProperty("hdfs.input.path.1");
        String workPath = app_conf.getProperty("hdfs.work.path");

        String cachePath = app_conf.getProperty("hdfs.cache.path");

        String outPath = app_conf.getProperty("hdfs.output.path.1");
        int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));
        int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
        String k_v_separator = app_conf.getProperty("hadoop.io.k_v_separator");
        //--------------------------------------------------------
        conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
        conf.set("mapred.textoutputformat.separator", k_v_separator);
        //--------------------------------------------------------
        job = new Job(conf);
        //加载图书标签信息
        FileSystem fs1 = FileSystem.get(conf);
        FileStatus[] status1 = fs1.globStatus(new Path(cachePath));
        for (int i = 0; i < status1.length; i++) {
            //DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()), conf);
        	job.addCacheFile(URI.create(status1[i].getPath().toString()));
            log.info("book_label_data file: " + status1[i].getPath().toString() + " has been add into distributed cache");
        }

        //job = new Job(conf);
        job.setJarByClass(UserLabelComputeDriver.class);

        //���������ַ
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        check(outPath, conf);

        //���������ַ
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        //����M-R
        job.setMapperClass(UserLabelComputeMapper.class);
        job.setNumReduceTasks(reduceNum);
        job.setReducerClass(UserLabelComputeReducer.class);

        //��������/�����ʽ
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //�����������
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //��־==================================================================================
        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        log.info("=================================================================================");

        return 0;
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

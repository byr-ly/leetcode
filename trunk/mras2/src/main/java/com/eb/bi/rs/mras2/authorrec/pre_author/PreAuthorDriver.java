package com.eb.bi.rs.mras2.authorrec.pre_author;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * Created by liyang on 2016/3/9.
 */
public class PreAuthorDriver extends Configured implements Tool{
    Logger log = Logger.getLogger(PreAuthorDriver.class);

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new PreAuthorDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户对作家的打分列表。
         ** 缓存：
         **     作家信息表。
         ** 输出：
         **		待预测的作家列表。
         ** 功能描述：
         ** 	找出需要预测得分的作家。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

//        String inputPath = "liyang/output/author_origin_score";
        String inputPath = "liyang/input/authorscore";
        String recOutputDir = "liyang/pre_author";
        String authorInfo = "liyang/author/author";

//        String inputPath = config.getParam("author_score_output_path", "/home/recsys/data/recsys_data/output/author_origin_score");
//        String recOutputDir = config.getParam("pre_author_output_path", "/home/recsys/data/recsys_data/output/pre_author");
//        String authorInfo = config.getParam("dim.dim_author", "/home/recsys/data/recsys_data/input/dim.dim_author");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("作家信息路径: " + authorInfo);

        //作家信息
        FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(authorInfo));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(PreAuthorDriver.class);
        job.setJobName("PreAuthor");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(PreAuthorMapper.class);
        job.setReducerClass(PreAuthorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));

        if (job.waitForCompletion(true)) {
//            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 0;
        } else {
//            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
//        return 0;
    }
}

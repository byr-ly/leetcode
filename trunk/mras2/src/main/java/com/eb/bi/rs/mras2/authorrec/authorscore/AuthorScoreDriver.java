package com.eb.bi.rs.mras2.authorrec.authorscore;

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
 * Created by admin on 2016/3/4.
 */
public class AuthorScoreDriver extends Configured implements Tool{


    //Logger log = PluginUtil.getInstance().getLogger();

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new AuthorScoreDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径： 用户对图书的打分。
         ** 缓存：
         **     图书信息表。
         ** 输出：
         **		用户对作家的打分。
         ** 功能描述：
         ** 	求用户对阅读过作家的打分。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String inputPath = "liyang/input/userscore";
        String recOutputDir = "liyang/output/author_origin_score";
        String prefBookInfo = "liyang/bookinfo/00000*";

//        String inputPath = config.getParam("user_book_score", "liyang/input");
//        String recOutputDir = config.getParam("author_score_output_path", "liyang/output/author_origin_score");
//        String prefBookInfo = config.getParam("bookinfo", "liyang/bookinfo/000000_0");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("图书信息路径: " + prefBookInfo);

        //图书信息
        FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(prefBookInfo));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(AuthorScoreDriver.class);
        job.setJobName("AuthorScore");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(AuthorScoreMapper.class);
        job.setReducerClass(AuthorScoreReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));

        if (job.waitForCompletion(true)) {
            return 0;
//            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
//            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
//        return 0;
    }
}

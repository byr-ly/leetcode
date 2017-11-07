package com.eb.bi.rs.mras2.authorrec.author_pre_score;

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
public class AuthorPreScoreDriver extends Configured implements Tool{
//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new AuthorPreScoreDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户待预测作家列表。
         ** 缓存：
         **     用户作家评分列表
         **     作家相似度列表
         **     作家平均分列表
         ** 输出：
         **		作家的预测分。
         ** 功能描述：
         ** 	计算作家的预测分。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

//        String inputPath = "liyang/authorPreScoreTest/pre_author";
//        String recOutputDir = "liyang/output/author_pre_score";
//        String authorScore = "liyang/authorPreScoreTest/authorscore/authorscore";
//        String authorSimilarity = "liyang/authorPreScoreTest/similarity/similarity";
//        String authorAveScore = "liyang/authorPreScoreTest/authorAveScore/authorAveScore";

        String inputPath = "liyang/pre_author";
        String recOutputDir = "liyang/output/author_pre_score";
        String authorScore = "liyang/input/authorscore/authorscore";
        String authorSimilarity = "liyang/output/similarity/similarity";
        String authorAveScore = "liyang/output/author_average_score/part*";

//        String inputPath = config.getParam("pre_author_output_path", "/home/recsys/data/recsys_data/output/pre_author");
//        String recOutputDir = config.getParam("author_pre_score_output_path", "/home/recsys/data/recsys_data/output/author_pre_score");
//        String authorScore = config.getParam("author_score_output_path","/home/recsys/data/recsys_data/output/author_origin_score");
//        String authorSimilarity = config.getParam("author_similarity_output_path", "/home/recsys/data/recsys_data/output/author_similarity");
//        String authorAveScore = config.getParam("author_average_score_output_path", "/home/recsys/data/recsys_data/output/author_average_score");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("用户作家评分列表： " + authorScore);
        System.out.println("作家相似度列表： " + authorSimilarity);
        System.out.println("作家平均分列表： " + authorAveScore);

        //用户作家评分
//        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(authorScore + "/part-*"));
        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(authorScore));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        //作家相似度
//        fs = FileSystem.get(conf).globStatus(new Path(authorSimilarity + "/part-*"));
        fs = FileSystem.get(conf).globStatus(new Path(authorSimilarity));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        //作家平均分
//        fs = FileSystem.get(conf).globStatus(new Path(authorAveScore + "/part-*"));
        fs = FileSystem.get(conf).globStatus(new Path(authorAveScore));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(AuthorPreScoreDriver.class);
        job.setJobName("AuthorPreScore");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(AuthorPreScoreMapper.class);
        job.setReducerClass(AuthorPreScoreReducer.class);

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

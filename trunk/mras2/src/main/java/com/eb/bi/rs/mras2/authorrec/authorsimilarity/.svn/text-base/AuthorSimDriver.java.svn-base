package com.eb.bi.rs.mras2.authorrec.authorsimilarity;

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
public class AuthorSimDriver extends Configured implements Tool{
//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new AuthorSimDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径： 作家共现用户列表。
         ** 缓存：
         **     用户对作家的打分列表。
         ** 输出：
         **		作家之间的相似度列表。
         ** 功能描述：
         ** 	计算作家之间的相似度。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String inputPath = "liyang/output/commom_user";
//        String inputPath = "liyang/input/commonusertest";
        String recOutputDir = "liyang/output/author_similarity";
        String bookScoreInfo = "liyang/input/authorscore/authorscore";
//        String bookScoreInfo = "liyang/output/author_origin_score/part*";

//        String inputPath = config.getParam("common_user_output_path", "/home/recsys/data/recsys_data/output/common_user");
//        String recOutputDir = config.getParam("author_similarity_output_path", "/home/recsys/data/recsys_data/output/author_similarity");
//        String bookScoreInfo = config.getParam("author_score_output_path", "/home/recsys/data/recsys_data/output/author_origin_score");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("用户作家打分路径: " + bookScoreInfo);

        //用户作家打分信息
        FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(bookScoreInfo));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(AuthorSimDriver.class);
        job.setJobName("AuthorSim");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(AuthorSimMapper.class);
        job.setReducerClass(AuthorSimReducer.class);

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

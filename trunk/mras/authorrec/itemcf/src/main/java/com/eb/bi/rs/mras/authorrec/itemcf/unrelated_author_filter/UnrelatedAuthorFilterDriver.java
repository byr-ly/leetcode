package com.eb.bi.rs.mras.authorrec.itemcf.unrelated_author_filter;

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

import java.net.URI;

/**
 * Created by liyang on 2016/3/16.
 */
public class UnrelatedAuthorFilterDriver extends Configured implements Tool {
//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new UnrelatedAuthorFilterDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户历史偏好表。
         ** 缓存：
         **     预测作家列表
         **     作家分类列表
         ** 输出：
         **		对于未关联作家的筛选结果。
         ** 功能描述：
         ** 	筛选出10个待推荐的未关联作家。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String inputPath = "liyang/unrelatedAuthorFilterTest/userPref";
        String recOutputDir = "liyang/output/unrelated_author_filter";
        String preAuthorInfo = "liyang/unrelatedAuthorFilterTest/preAuthor/preAuthor";
        String authorTypeInfo = "liyang/unrelatedAuthorFilterTest/authorType/authorType";

//        String inputPath = config.getParam("user_pref_path", "/home/recsys/data/recsys_data/input/dmn.irecm_us_pref_all");
//        String recOutputDir = config.getParam("unrelated_author_filter_output_path", "/home/recsys/data/recsys_data/output/unrelated_author_filter");
//        String preAuthorInfo = config.getParam("pre_author_output_path", "/home/recsys/data/recsys_data/output/pre_author");
//        String authorTypeInfo = config.getParam("author_type_output_path", "/home/recsys/data/recsys_data/output/author_type");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("预测作家列表： " + preAuthorInfo);
        System.out.println("作家分类列表： " + authorTypeInfo);

        //图书信息列表
//        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(preAuthorInfo + "/part-*"));
        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(preAuthorInfo));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        //智能推荐基础数据
//        fs = FileSystem.get(conf).globStatus(new Path(authorTypeInfo + "/part-*"));
        fs = FileSystem.get(conf).globStatus(new Path(authorTypeInfo));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(UnrelatedAuthorFilterDriver.class);
        job.setJobName("UnrelatedAuthorFiter");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(UnrelatedAuthorFilterMapper.class);
        job.setReducerClass(UnrelatedAuthorFilterReducer.class);

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

package com.eb.bi.rs.mras.authorrec.itemcf.authortype;

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
 * Created by liyang on 2016/3/8.
 */
public class AuthorTypeDriver extends Configured implements Tool{

//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new AuthorTypeDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径： 图书推荐库表（图书分类表）。
         ** 缓存：
         **     图书信息表。
         ** 输出：
         **		作家分类表。
         ** 功能描述：
         ** 	求作家的分类以及作家在此分类下图书的数量。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String inputPath = "liyang/input/booktype";
        String recOutputDir = "liyang/output/author_type";
        String prefBookInfo = "liyang/bookinfo/00000*";

//        String inputPath = config.getParam("book_type", "/home/recsys/data/recsys_data/input/book_type");
//        String recOutputDir = config.getParam("author_type_output_path", "/home/recsys/data/recsys_data/output/author_type");
//        String prefBookInfo = config.getParam("bookinfo", "/home/recsys/data/recsys_data/input/dim.dim_bookinfo");

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
        job.setJarByClass(AuthorTypeDriver.class);
        job.setJobName("AuthorType");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(AuthorTypeMapper.class);
        job.setReducerClass(AuthorTypeReducer.class);

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

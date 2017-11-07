package com.eb.bi.rs.mras.authorrec.itemcf.read_author_filter;

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
 * Created by liyang on 2016/3/15.
 */
public class ReadAuthorFilterDriver extends Configured implements Tool{
//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new ReadAuthorFilterDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户作家评分列表。
         ** 缓存：
         **     图书信息表
         **     智能推荐基础数据
         ** 输出：
         **		对于阅读过作家的筛选结果。
         ** 功能描述：
         ** 	筛选出10个待推荐的已阅读作家。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

//        String inputPath = "liyang/readAuthorFilterTest/authorscore";
//        String recOutputDir = "liyang/output/read_author_filter";
//        String prefBookInfo = "liyang/readAuthorFilterTest/bookinfo/book";
//        String basicData = "liyang/readAuthorFilterTest/base/base";

        String inputPath = "liyang/input/authorscore";
        String recOutputDir = "liyang/output/read_author_filter";
        String prefBookInfo = "liyang/bookinfo/000000_0";
        String basicData = "liyang/base/base";

//        String inputPath = config.getParam("author_score_output_path", "/home/recsys/data/recsys_data/output/author_origin_score");
//        String recOutputDir = config.getParam("read_author_filter_output_path", "/home/recsys/data/recsys_data/output/read_author_filter");
//        String prefBookInfo = config.getParam("bookinfo", "/home/recsys/data/recsys_data/input/dim.dim_bookinfo");
//        String basicData = config.getParam("bkid_6cm", "/home/recsys/data/recsys_data/input/dmn.irecm_bkid_6cm ");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("图书信息列表： " + prefBookInfo);
        System.out.println("智能推荐基础数据： " + basicData);

        //图书信息列表
        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(prefBookInfo));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        //智能推荐基础数据
        fs = FileSystem.get(conf).globStatus(new Path(basicData));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf);
        job.setJarByClass(ReadAuthorFilterDriver.class);
        job.setJobName("ReadAuthorFiter");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(ReadAuthorFilterMapper.class);
        job.setReducerClass(ReadAuthorFilterReducer.class);

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

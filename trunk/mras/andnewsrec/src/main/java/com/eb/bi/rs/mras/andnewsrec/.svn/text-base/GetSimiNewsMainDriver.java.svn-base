package com.eb.bi.rs.mras.andnewsrec;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras.andnewsrec.getRetsAndFilter.GetRetsAndFilterMapper;
import com.eb.bi.rs.mras.andnewsrec.getRetsAndFilter.GetRetsAndFilterReducer;
import com.eb.bi.rs.mras.andnewsrec.get_idf_value.GetIdfValueMapper;
import com.eb.bi.rs.mras.andnewsrec.get_idf_value.GetIdfValueReducer;
import com.eb.bi.rs.mras.andnewsrec.get_keyword_news.GetKeyWordNewsMapper;
import com.eb.bi.rs.mras.andnewsrec.get_keyword_news.GetKeyWordNewsReducer;
import com.eb.bi.rs.mras.andnewsrec.get_news_num.GetNewsNumMapper;
import com.eb.bi.rs.mras.andnewsrec.get_news_num.GetNewsNumReducer;
import com.eb.bi.rs.mras.andnewsrec.load_allnews.LoadNewsDriver;
import com.eb.bi.rs.mras.andnewsrec.load_allnews.LoadNewsMapper;
import com.eb.bi.rs.mras.andnewsrec.load_allnews.LoadNewsReducer;
import com.eb.bi.rs.mras.andnewsrec.news_word_transpose.NewsWordsTransposeMapper;
import com.eb.bi.rs.mras.andnewsrec.news_word_transpose.NewsWordsTransposeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LiMingji on 2016/3/22.
 */
public class GetSimiNewsMainDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new GetSimiNewsMainDriver(), args);

        Date dateEnd = new Date();
        long timeCost = dateEnd.getTime() - dateBeg.getTime();

        PluginResult result = PluginUtil.getInstance().getResult();
        result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(ms) :" + timeCost);
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        Logger log = PluginUtil.getInstance().getLogger();
        PluginConfig config = PluginUtil.getInstance().getConfig();

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		和新闻新闻数据。newsID|classID|title|content
         ** 输出：
         **		新闻数量  newsNum: number
         ** 功能描述：
         ** 	读入所有新闻，并计算新闻的数量。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String news_input_path = config.getParam("news_input_path", "liyang/andnews/input");
        String news_num = config.getParam("news_num", "liyang/andnews/output/news_num");

        Job job = new Job(conf, "newsNum");
        job.setJarByClass(GetSimiNewsMainDriver.class);

        //检查输出目录
        checkOutputPath(news_num);
        System.out.println("输入路径: " + news_input_path);
        System.out.println("输出路径: " + news_num);

        FileInputFormat.addInputPath(job, new Path(news_input_path));
        FileOutputFormat.setOutputPath(job, new Path(news_num));

        job.setMapperClass(GetNewsNumMapper.class);
        job.setReducerClass(GetNewsNumReducer.class);

        job.setNumReduceTasks(config.getParam("get_news_num_reduce_task_num", 1));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }


        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		和新闻新闻数据。newsID|classID|title|content
         ** 输出：
         **		词-idf值  word|idfValue
         ** 缓存：
         **     新闻数量值
         ** 功能描述：
         ** 	读入所有新闻，并计算新闻中每个词的idf值。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");

        String word_idf_value = config.getParam("word_idf_value", "liyang/andnews/output/word_idf_value");
        String wordsTopN = config.getParam("news_word_top_N", "30");

        System.out.println("=================读入全部新闻=================");

        //检查输出目录
        checkOutputPath(word_idf_value);
        System.out.println("输入路径: " + news_input_path);
        System.out.println("输出路径: " + word_idf_value);
        System.out.println("缓存路径: " + news_num);

        FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(news_num + "/part*"));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf, "GetIdfValue");
        job.setJarByClass(GetSimiNewsMainDriver.class);

        FileInputFormat.addInputPath(job, new Path(news_input_path));
        FileOutputFormat.setOutputPath(job, new Path(word_idf_value));

        job.setMapperClass(GetIdfValueMapper.class);
        job.setReducerClass(GetIdfValueReducer.class);

        job.setNumReduceTasks(config.getParam("get_idf_value_reduce_task_num", 1));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		和新闻新闻数据。newsID|classID|title|content
         ** 输出：
         **		新闻-词-权重  newsID  word,weight|word,weight|...
         ** 功能描述：
         ** 	读入所有新闻，并计算每个新闻下的TF-IDF权重。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");

        String word_weight_output = config.getParam("word_weight_output", "liyang/andnews/output/word_weight");

        //检查输出目录
        checkOutputPath(word_weight_output);
        System.out.println("输入路径: " + news_input_path);
        System.out.println("输出路径: " + word_weight_output);
        System.out.printf("缓存路径：" + word_idf_value);

        fs = FileSystem.get(conf).globStatus(new Path(word_idf_value + "/part*"));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf, "LoadNews");
        job.setJarByClass(LoadNewsDriver.class);

        conf.set("wordsTopN", wordsTopN);

        FileInputFormat.addInputPath(job, new Path(news_input_path));
        FileOutputFormat.setOutputPath(job, new Path(word_weight_output));

        job.setMapperClass(LoadNewsMapper.class);
        job.setReducerClass(LoadNewsReducer.class);

        job.setNumReduceTasks(config.getParam("load_allnews_reduce_task_num", 1));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		新闻-词-权重  newsID|classID  word,weight|word,weight|...
         ** 输出：
         **		词, 新闻-新闻-相似度  word|classID, newsID,simiNewsID,simi
         ** 功能描述：
         ** 	转置上述矩阵。并计算同一词下的新闻相似度
         **
         **
         *******************************************************************************************/
        log.info("=================================================================================");
//        job = Job.getInstance(conf, "NewsWordsTranspose");
        job = new Job(conf, "NewsWordsTranspose");
        job.setJarByClass(GetSimiNewsMainDriver.class);
        job.getConfiguration().setInt("mapred.task.timeout", 600000);

        String transpose_output = config.getParam("transpose_output", "liyang/andnews/output/news_word_transpose");

        System.out.println("输入路径: " + word_weight_output);
        System.out.println("输出路径: " + transpose_output);

        checkOutputPath(transpose_output);
        FileInputFormat.addInputPath(job, new Path(word_weight_output));
        FileOutputFormat.setOutputPath(job, new Path(transpose_output));

        job.setMapperClass(NewsWordsTransposeMapper.class);
        job.setReducerClass(NewsWordsTransposeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(config.getParam("transpose_reduce_task_num", 10));
        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		新闻-词-权重  newsID|classID  word,weight|word,weight|...
         ** 输出：
         **		词, 新闻-权重  word|classID, newsID,weight
         ** 功能描述：
         ** 	转置上述矩阵。
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        job = new Job(conf, "TransposeOnly");
        job.setJarByClass(GetSimiNewsMainDriver.class);
        job.getConfiguration().setInt("mapred.task.timeout", 600000);

        String transpose_only_output = config.getParam("transpose_only_output", "liyang/andnews/output/news_word_transpose");

        //检查输出目录
        checkOutputPath(transpose_only_output);
        System.out.println("输入路径: " + word_weight_output);
        System.out.println("输出路径: " + transpose_only_output);

        FileInputFormat.addInputPath(job, new Path(word_weight_output));
        FileOutputFormat.setOutputPath(job, new Path(transpose_only_output));

        job.setMapperClass(GetKeyWordNewsMapper.class);
        job.setReducerClass(GetKeyWordNewsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(config.getParam("transpose_only_reduce_task_num", 10));

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		词, 新闻-新闻-相似度  word|classID, newsID,simiNewsID,simi
         ** 输出：
         **		新闻相似新闻 newsID  newsID,scores|newsID,scores...
         ** 缓存：
         **     和新闻新闻数据。newsID|classID|title|content
         ** 功能描述：
         ** 	计算新闻的相似新闻，并取TOP
         **
         **
         *******************************************************************************************/

        System.out.println("=================计算推荐新闻，并取TOP=================");
//        job = Job.getInstance(conf, "GetRetsAndFilter");

        String simi_ret_output = config.getParam("simi_ret_output", "liyang/andnews/output/rets_output");
        String topN = config.getParam("top_news_N", "10");
        conf.set("top_news_N", topN);

        System.out.println("输入路径: " + transpose_output);
        System.out.println("输出路径: " + simi_ret_output);
//        System.out.println("缓存路径: " + news_input_path);
//
//        fs = FileSystem.get(conf).globStatus(new Path(news_input_path + "/00*"));
//
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }

        job = new Job(conf);
        job.setJarByClass(GetSimiNewsMainDriver.class);
        checkOutputPath(simi_ret_output);
        FileInputFormat.addInputPath(job, new Path(transpose_output));
        FileOutputFormat.setOutputPath(job, new Path(simi_ret_output));

        job.setMapperClass(GetRetsAndFilterMapper.class);
        job.setReducerClass(GetRetsAndFilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(config.getParam("get_simi_reduce_task_num", 10));
        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        return 0;
    }

    private void checkOutputPath(String fileName) {
        Logger log = PluginUtil.getInstance().getLogger();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path path = new Path(fileName);
            boolean isExists = fs.exists(path);
            if (isExists) {
                boolean isDel = fs.delete(path, true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}

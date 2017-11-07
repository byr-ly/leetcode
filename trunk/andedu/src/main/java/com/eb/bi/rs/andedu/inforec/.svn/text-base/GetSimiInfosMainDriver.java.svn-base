package com.eb.bi.rs.andedu.inforec;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.andedu.inforec.filler_infos.FillerInfosMapper;
import com.eb.bi.rs.andedu.inforec.filler_infos.FillerInfosReducer;
import com.eb.bi.rs.andedu.inforec.getRetsAndFilter.GetRetsAndFilterMapper;
import com.eb.bi.rs.andedu.inforec.getRetsAndFilter.GetRetsAndFilterReducer;
import com.eb.bi.rs.andedu.inforec.get_idf_value.GetIdfValueMapper;
import com.eb.bi.rs.andedu.inforec.get_idf_value.GetIdfValueReducer;
import com.eb.bi.rs.andedu.inforec.get_keyword_infos.GetKeyWordInfosMapper;
import com.eb.bi.rs.andedu.inforec.get_keyword_infos.GetKeyWordInfosReducer;
import com.eb.bi.rs.andedu.inforec.get_infos_num.GetInfosNumMapper;
import com.eb.bi.rs.andedu.inforec.get_infos_num.GetInfosNumReducer;
import com.eb.bi.rs.andedu.inforec.load_allinfos.LoadInfosDriver;
import com.eb.bi.rs.andedu.inforec.load_allinfos.LoadInfosMapper;
import com.eb.bi.rs.andedu.inforec.load_allinfos.LoadInfosReducer;
import com.eb.bi.rs.andedu.inforec.infos_word_transpose.InfosWordsTransposeMapper;
import com.eb.bi.rs.andedu.inforec.infos_word_transpose.InfosWordsTransposeReducer;

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
public class GetSimiInfosMainDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new GetSimiInfosMainDriver(), args);

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
        FileStatus[] status;
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String infos_input_path = config.getParam("infos_input_path", "liyang/andnews/input");
        String infos_num = config.getParam("infos_num", "liyang/andnews/output/news_num");
//        String filterChar = config.getParam("filterChar", "[\\pP��$^=+~|│④Ⅰ1<>\r\n]");
//        conf.set("filterChar", filterChar);
        String segMethod = config.getParam("segMethod", "N");
        conf.set("segMethod", segMethod);

        Job job = new Job(conf, "infosNum");
        job.setJarByClass(GetSimiInfosMainDriver.class);

        //检查输出目录
        checkOutputPath(infos_num);
        System.out.println("输入路径: " + infos_input_path);
        System.out.println("输出路径: " + infos_num);

        FileInputFormat.addInputPath(job, new Path(infos_input_path));
        FileOutputFormat.setOutputPath(job, new Path(infos_num));

        job.setMapperClass(GetInfosNumMapper.class);
        job.setReducerClass(GetInfosNumReducer.class);

        job.setNumReduceTasks(config.getParam("get_infos_num_reduce_task_num", 1));

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
        String wordsTopN = config.getParam("infos_word_top_N", "30");

        System.out.println("=================读入全部新闻=================");

        //检查输出目录
        checkOutputPath(word_idf_value);
        System.out.println("输入路径: " + infos_input_path);
        System.out.println("输出路径: " + word_idf_value);
        System.out.println("缓存路径: " + infos_num);

//        FileStatus[] fs = FileSystem.get(conf).globStatus(new Path(news_num + "/part*"));
//
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }

        job = new Job(conf, "GetIdfValue");

        status = FileSystem.get(conf).globStatus(new Path(infos_num + "/part-*"));
        for (int i = 0; i < status.length; i++) {
			/*DistributedCache修改点*/
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());

//			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
            log.info("news num file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }

        job.setJarByClass(GetSimiInfosMainDriver.class);

        FileInputFormat.addInputPath(job, new Path(infos_input_path));
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
        String titleTimes = config.getParam("titleTimes", "1");
        String contentTimes = config.getParam("contentTimes", "1");
        conf.set("titleTimes", titleTimes);
        conf.set("contentTimes", contentTimes);
        
        //检查输出目录
        checkOutputPath(word_weight_output);
        System.out.println("输入路径: " + infos_input_path);
        System.out.println("输出路径: " + word_weight_output);
        System.out.printf("缓存路径：" + word_idf_value);

//        fs = FileSystem.get(conf).globStatus(new Path(word_idf_value + "/part*"));
//
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }

        job = new Job(conf, "LoadInfos");

        status = FileSystem.get(conf).globStatus(new Path(word_idf_value + "/part-*"));
        for (int i = 0; i < status.length; i++) {
			/*DistributedCache修改点*/
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());

//			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
            log.info("word idf value file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }

        job.setJarByClass(LoadInfosDriver.class);

        conf.set("wordsTopN", wordsTopN);

        FileInputFormat.addInputPath(job, new Path(infos_input_path));
        FileOutputFormat.setOutputPath(job, new Path(word_weight_output));

        job.setMapperClass(LoadInfosMapper.class);
        job.setReducerClass(LoadInfosReducer.class);

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
        job = new Job(conf, "InfosWordsTranspose");
        job.setJarByClass(GetSimiInfosMainDriver.class);
        job.getConfiguration().setInt("mapred.task.timeout", 600000);

        String transpose_output = config.getParam("transpose_output", "liyang/andnews/output/news_word_transpose");

        System.out.println("输入路径: " + word_weight_output);
        System.out.println("输出路径: " + transpose_output);

        checkOutputPath(transpose_output);
        FileInputFormat.addInputPath(job, new Path(word_weight_output));
        FileOutputFormat.setOutputPath(job, new Path(transpose_output));

        job.setMapperClass(InfosWordsTransposeMapper.class);
        job.setReducerClass(InfosWordsTransposeReducer.class);

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
        job.setJarByClass(GetSimiInfosMainDriver.class);
        job.getConfiguration().setInt("mapred.task.timeout", 600000);

        String transpose_only_output = config.getParam("transpose_only_output", "liyang/andnews/output/news_word_transpose");

        //检查输出目录
        checkOutputPath(transpose_only_output);
        System.out.println("输入路径: " + word_weight_output);
        System.out.println("输出路径: " + transpose_only_output);

        FileInputFormat.addInputPath(job, new Path(word_weight_output));
        FileOutputFormat.setOutputPath(job, new Path(transpose_only_output));

        job.setMapperClass(GetKeyWordInfosMapper.class);
        job.setReducerClass(GetKeyWordInfosReducer.class);
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
        String topN = config.getParam("top_infos_N", "10");
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
        job.setJarByClass(GetSimiInfosMainDriver.class);
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
        
//        /**
//         * ********************************************************************************************
//         * MAP REDUCE JOB:
//         ** 输入：
//         **		和新闻新闻数据。资讯idchr(001)）资讯标题chr(001)图片封面地址chr(001)来源chr(001)浏览量chr(001)生成的静态页面的URL地址chr(001)发布时间yyyymmddhhMMsschr(001)）省份id,多个用逗号隔开chr(001)栏目id (chr(001)为分隔符)
//         ** 输出：
//         **		id-排序资讯  id  资讯id|资讯id|资讯id...
//         ** 功能描述：
//         ** 	读入所有新闻所有内容，选出最新最热资讯。
//         **
//         **
//         *******************************************************************************************/
//
//        log.info("=================================================================================");
//
//        String infos_input = config.getParam("infos_input", "/user/recsys/pub_data/infomation");
//        String filler_output = config.getParam("filler_output", "/user/recsys/job0007/filler_output");
//        String fillerTime = config.getParam("fillerTime", "20170821");
//        String fillerTopN = config.getParam("fillerTopN", "100");
//        conf.set("fillerTime", fillerTime);
//        conf.set("fillerTopN", fillerTopN);
//        
//        //检查输出目录
//        checkOutputPath(filler_output);
//        System.out.println("输入路径: " + infos_input);
//        System.out.println("输出路径: " + filler_output);
//
////        fs = FileSystem.get(conf).globStatus(new Path(word_idf_value + "/part*"));
////
////        for (int i = 0; i < fs.length; i++) {
////            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
////            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
////        }
//
//        job = new Job(conf, "FillerInfos");
//
//        job.setJarByClass(GetSimiInfosMainDriver.class);
//
//        FileInputFormat.addInputPath(job, new Path(infos_input));
//        FileOutputFormat.setOutputPath(job, new Path(filler_output));
//
//        job.setMapperClass(FillerInfosMapper.class);
//        job.setReducerClass(FillerInfosReducer.class);
//
//        job.setNumReduceTasks(config.getParam("fillter_infos_reduce_task_num", 1));
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        if (job.waitForCompletion(true)) {
//            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
//        } else {
//            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
//            return 1;
//        }
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

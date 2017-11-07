package com.eb.bi.rs.mras.unifyrec.attributeengin;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras.unifyrec.attributeengin.ComprehensivePrefRecResult.ComprehensivePrefRecResultReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.ForceRecClassPref.ForceRecClassPrefReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.PrefRecResultMerge.PrefRecResultMergeMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.PrefRecResultMerge.PrefRecResultMergeReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.SeparatePrefRecResult.SeparatePrefRecResultReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_PrefVectorMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_ReadClassesMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_ReadHistoryMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserClassPref.UserClassPrefDriver;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserClassPref.UserClassPrefReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator.UserPrefGeneratorMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator.UserPrefGeneratorReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator.UserPrefVector;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadClasses.UserReadClassesMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadClasses.UserReadClassesReducer;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadHistory.UserReadHistoryMapper;
import com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadHistory.UserReadHistoryReducer;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LiuJie on 2016/04/20.
 */
public class BookPrefEngineDay extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new BookPrefEngineDay(), args);

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
    public int run(String[] strings) throws Exception {
        Logger log = PluginUtil.getInstance().getLogger();
        PluginConfig config = PluginUtil.getInstance().getConfig();
        
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径： 用户偏好向量。
         ** 缓存：
         **     深度用户向量的均值，上一步的输出。
         **     用户偏好向量的相似向量。
         ** 输出：
         **		用户偏好向量减去深度用户向量的均值
         ** 功能描述：
         ** 	求用户偏好向量
         **
         **
         *******************************************************************************************/
        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String prefAllPath = config.getParam("user_pref_all", "/home/recsys/data/recsys_data/attribute_engine_increment/input/user_pref_all");
        String recEngineOutputDir = config.getParam("user_pref_vector", "/home/recsys/data/recsys_data/output/user_pref_vector");
        String prefAverage = config.getParam("user_pref_average", "/user/hadoop/liujie/test/output/user_pref_average");
        String simClassPath = config.getParam("sim_classes", "/home/recsys/data/recsys_data/attribute_engine/input/simclass");

        System.out.println("输入路径: " + prefAllPath);
        System.out.println("输出路径: " + recEngineOutputDir);
        System.out.println("平均值路径: " + prefAverage);
        System.out.println("相似分类路径： " + simClassPath);

        //相似分类表
        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(simClassPath));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        //均值
        fs = FileSystem.get(conf).globStatus(new Path(prefAverage + "/part-*"));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        Job job = new Job(conf, "unifyrec_prefengineincrement_user-pref-generator");
        job.setJarByClass(BookPrefEngineDay.class);

        //检查输出目录
        checkOutputPath(recEngineOutputDir);

        FileInputFormat.setInputPaths(job, new Path(prefAllPath));
        FileOutputFormat.setOutputPath(job, new Path(recEngineOutputDir));

        job.setMapperClass(UserPrefGeneratorMapper.class);
        job.setReducerClass(UserPrefGeneratorReducer.class);

        job.setNumReduceTasks(config.getParam("user_vector_generator_reduce_task_num", 10));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserPrefVector.class);
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
         **		1.输入路径： 用户历史行为。
         ** 输出：
         **		用户阅读过的所有分类。
         ** 功能描述：
         ** 	求出用户所阅读过的所有分类。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String inputPath = config.getParam("user_bookid_all", "/home/recsys/data/recsys_data/attribute_engine_increment/input/user_bookid_all");
        String outputPath = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + outputPath);

        job = new Job(conf, "unifyrec_prefengineincrement_user-read-classes");
        job.setJarByClass(BookPrefEngineDay.class);

        checkOutputPath(outputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(UserReadClassesMapper.class);
        job.setReducerClass(UserReadClassesReducer.class);
        job.setNumReduceTasks(config.getParam("user_read_class_reduce_task_num", 10));

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
         **		1.输入路径： 用户历史图书。
         ** 输出：
         **		用户的所有历史图书及黑名单图书。
         ** 功能描述：
         ** 	求出用户所阅读过的所有图书。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        inputPath = config.getParam("user_read_his", "/user/recsys/user_read_history");
        outputPath = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + outputPath);

        job = new Job(conf, "unifyrec_prefengineincrement_user-read-history");
        job.setJarByClass(BookPrefEngineDay.class);

        checkOutputPath(outputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(UserReadHistoryMapper.class);
        job.setReducerClass(UserReadHistoryReducer.class);
        job.setNumReduceTasks(config.getParam("user_read_history_reduce_task_num", 30));

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
         **		1.输入路径： 用户向量
         **     2.用户阅读过的分类
         ** 输出：
         **	       用户分类偏好表        字段：msisdn用户 |分群|偏执类型|前三分类 |相似分类 |历史阅读分类 
         ** 功能描述：
         ** 	构建用户分类偏好表，为咪咕抢发测试使用
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector");
        String bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes");
        String outputDir = config.getParam("user_class_pref", "/user/hadoop/liujie/test/output/user_class_pref");

        job = new Job(conf,"unifyrec_prefengineincrement_user-class-pref");
        job.setJarByClass(UserClassPrefDriver.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
    
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("user_class_pref_reduce_task_num",60));
        job.setReducerClass(UserClassPrefReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 用户历史偏好。
         **     2.用户阅读过的分类
         ** 输出：
         **		用户对图书的分类偏好打分。
         ** 缓存：
         *      各个分版的编辑强推图书
         ** 功能描述：
         ** 	求用户对编辑强推图书的分类偏好打分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        outputDir = config.getParam("forcerec_classpref_result", "/home/recsys/data/recsys_data/output/forcerec_classpref_result_increment");
        String manRecomBooks = config.getParam("man_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/man_force_recom_book");
        String femaleRecomBooks = config.getParam("female_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/female_force_recom_book");
        String publishRecomBooks = config.getParam("publish_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/publish_force_recom_book");

        System.out.println("男版编辑强推库图书信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(manRecomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        System.out.println("女版编辑强推库图书信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(femaleRecomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        System.out.println("出版编辑强推库图书信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(publishRecomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        job = new Job(conf, "unifyrec_prefengineincrement_force-rec-class-pref");
        job.setJarByClass(BookPrefEngineWeek.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("forcerec_classpref_reduce_task_num", 60));
        job.setReducerClass(ForceRecClassPrefReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 用户偏好向量。
         **     2.用户阅读过的分类
         ** 输出：
         **		用户对 综合版 图书的打分。
         ** 缓存：
         *      图书雅俗对照表
         *      综合版推荐图书库
         ** 功能描述：
         ** 	求用户对综合版推荐图书的打分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        outputDir = config.getParam("comprehensive_prefrec_result", "/user/hadoop/liujie/test/output/comprehensive_prefrec_result_increment");
        String bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
        String recomBooks = config.getParam("recom_bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/recom_bookinfo");
        String userReadHistory = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");

        System.out.println("雅俗表信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(bookClassifierScores));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        System.out.println("推荐图书表加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(recomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        conf.set("comprehensive_prefrec_result_reduce_top", config.getParam("comprehensive_prefrec_result_reduce_top", "2000"));

        job = new Job(conf, "unifyrec_prefengineincrement_comprehensive-pref-recommend");
        job.setJarByClass(BookPrefEngineDay.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(userReadHistory), TextInputFormat.class, User_ReadHistoryMapper.class);
        
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("comprehensive_prefrec_result_reduce_task_num", 100));
        job.setReducerClass(ComprehensivePrefRecResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 用户偏好向量。
         **     2.用户阅读过的分类
         ** 输出：
         **		男生版  用户对图书的打分。
         ** 缓存：
         *      图书雅俗对照表
         *      男生版 推荐图书库
         ** 功能描述：
         ** 	求用户对推荐图书的打分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        
        userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        outputDir = config.getParam("man_prefrec_result", "/user/hadoop/liujie/test/output/man_prefrec_result_increment");
        bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
        recomBooks = config.getParam("man_recom_bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/man_recom_bookinfo");
        userReadHistory = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");


        System.out.println("雅俗表信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(bookClassifierScores));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        System.out.println("推荐图书表加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(recomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf, "unifyrec_prefengineincrement_man-pref-recommend");
        job.setJarByClass(BookPrefEngineDay.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(userReadHistory), TextInputFormat.class, User_ReadHistoryMapper.class);
        
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("separate_prefrec_result_reduce_task_num", 100));
        job.setReducerClass(SeparatePrefRecResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 用户偏好向量。
         **     2.用户阅读过的分类
         ** 输出：
         **		女生版 用户对图书的打分。
         ** 缓存：
         *      图书雅俗对照表
         *      女生版 推荐图书库
         ** 功能描述：
         ** 	求用户对推荐图书的打分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        outputDir = config.getParam("female_prefrec_result", "/user/hadoop/liujie/test/output/female_prefrec_result_increment");
        bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
        recomBooks = config.getParam("female_recom_bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/female_recom_bookinfo");
        userReadHistory = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");

        System.out.println("雅俗表信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(bookClassifierScores));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        System.out.println("推荐图书表加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(recomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        job = new Job(conf, "unifyrec_prefengineincrement_female-pref-recommend");
        job.setJarByClass(BookPrefEngineDay.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(userReadHistory), TextInputFormat.class, User_ReadHistoryMapper.class);
        
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("separate_prefrec_result_reduce_task_num", 100));
        job.setReducerClass(SeparatePrefRecResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 用户偏好向量。
         **     2.用户阅读过的分类
         ** 输出：
         **		出版版 用户对图书的打分。
         ** 缓存：
         *      图书雅俗对照表
         *      出版版 推荐图书库
         ** 功能描述：
         ** 	求用户对推荐图书的打分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        outputDir = config.getParam("publish_prefrec_result", "/user/hadoop/liujie/test/output/publish_prefrec_result_increment");
        bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
        recomBooks = config.getParam("publish_recom_bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/publish_recom_bookinfo");
        userReadHistory = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");
        
        String isDug = config.getParam("separate_prefrec_result_reduce_debug", "false");
        conf.setBoolean("separate_prefrec_result_reduce_debug", Boolean.parseBoolean(isDug));
        
        System.out.println("雅俗表信息加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(bookClassifierScores));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        System.out.println("推荐图书表加载到DistributedCache");
        fs = FileSystem.get(conf).listStatus(new Path(recomBooks));
        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }


        job = new Job(conf, "unifyrec_prefengineincrement_publish-pref-recommend");
        job.setJarByClass(BookPrefEngineDay.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(userReadHistory), TextInputFormat.class, User_ReadHistoryMapper.class);
        
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("separate_prefrec_result_reduce_task_num", 100));
        job.setReducerClass(SeparatePrefRecResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
         **		1.输入路径： 综合版及分版偏好推荐结果。
         ** 输出：
         **		去重后的推荐结果合集。
         ** 功能描述：
         ** 	求出各个版推荐结果的合集。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String inputPath1 = config.getParam("comprehensive_prefrec_result", "/user/hadoop/liujie/test/output/comprehensive_prefrec_result_increment");
        String inputPath2 = config.getParam("man_prefrec_result", "/user/hadoop/liujie/test/output/man_prefrec_result_increment");
        String inputPath3 = config.getParam("female_prefrec_result", "/user/hadoop/liujie/test/output/female_prefrec_result_increment");
        String inputPath4 = config.getParam("publish_prefrec_result", "/user/hadoop/liujie/test/output/publish_prefrec_result_increment");
        
        outputPath = config.getParam("prefrec_merge_result", "/user/hadoop/liujie/test/output/prefrec_merge_result_increment");

        System.out.println("输入路径1: " + inputPath1);
        System.out.println("输入路径2: " + inputPath2);
        System.out.println("输入路径3: " + inputPath3);
        System.out.println("输入路径4: " + inputPath4);
        System.out.println("输出路径: " + outputPath);

        job = new Job(conf, "unifyrec_prefengineincrement_prefrec-result-merge");
        job.setJarByClass(BookPrefEngineDay.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath3), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath4), TextInputFormat.class, PrefRecResultMergeMapper.class);
        //输出目录
        checkOutputPath(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(PrefRecResultMergeReducer.class);
        job.setNumReduceTasks(config.getParam("prerec_result_merge_reduce_task_num", 300));

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

package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep.UserVectorReducer;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep.User_BaseInfoMapper;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep.User_PrefAverageMapper;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep.User_PrefGeneratorMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_PrefVectorMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_ReadClassesMapper;
import com.eb.bi.rs.mras2.unifyrec.UserPrefAverage.UserPrefAverageReducer;
import com.eb.bi.rs.mras2.unifyrec.UserPrefGenerator.UserPrefGeneratorReducer;
import com.eb.bi.rs.mras2.unifyrec.UserPrefGenerator.UserPrefVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * Created by LiuJie on 2017/08/03.
 */
public class UserVectorPrepDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new UserVectorPrepDriver(), args);

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
         ** 输出：
         **		深度用户的偏好向量均值。
         ** 功能描述：
         ** 	以深度用户作为用户代表。求用户偏好向量的均值。
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        FileStatus[] status;
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String prefAllPath = config.getParam("user_pref_all", "/home/recsys/data/recsys_data/attribute_engine/input/user_pref_all");
        String prefAverageOutputPath = config.getParam("user_pref_average", "/user/hadoop/liujie/test/output/user_pref_average");

        System.out.println("输入路径: " + prefAllPath);
        System.out.println("输出路径: " + prefAverageOutputPath);

        Job job = Job.getInstance(conf, "unifyrec_prefengine_user-pref-average");
        job.setJarByClass(UserVectorPrepDriver.class);

        //检查输出目录
        checkOutputPath(prefAverageOutputPath);
        FileInputFormat.setInputPaths(job, new Path(prefAllPath));
        FileOutputFormat.setOutputPath(job, new Path(prefAverageOutputPath));

        job.setMapperClass(User_PrefAverageMapper.class);
        job.setReducerClass(UserPrefAverageReducer.class);

        job.setNumReduceTasks(config.getParam("user_pref_average_reduce_task_num", 1));
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
         **		1.输入路径： 用户偏好向量。
         ** 缓存：
         **     深度用户向量的均值，上一步的输出。
         **     用户偏好向量的相似向量。
         ** 输出：
         **		用户偏好向量减去深度用户向量的均值
         ** 功能描述：
         ** 	得到用户偏好向量+前三偏好分类的相似分类
         **
         **
         *******************************************************************************************/
        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        prefAllPath = config.getParam("user_pref_all", "");
        String recEngineOutputDir = config.getParam("user_pref_vector", "");
        String prefAverage = config.getParam("user_pref_average", "");
        String simClassPath = config.getParam("sim_classes", "");

        System.out.println("输入路径: " + prefAllPath);
        System.out.println("输出路径: " + recEngineOutputDir);
        System.out.println("平均值路径: " + prefAverage);
        System.out.println("相似分类路径： " + simClassPath);

        job = Job.getInstance(conf, "unifyrec_prefengineincrement_user-pref-generator");

        status = FileSystem.get(conf).globStatus(new Path(simClassPath));
        for (int i = 0; i < status.length; i++) {
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());
            log.info("cache file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }

        status = FileSystem.get(conf).globStatus(new Path(prefAverage + "/part-*"));
        for (int i = 0; i < status.length; i++) {
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());
            log.info("cache file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }

        job.setJarByClass(UserVectorPrepDriver.class);

        //检查输出目录
        checkOutputPath(recEngineOutputDir);

        FileInputFormat.setInputPaths(job, new Path(prefAllPath));
        FileOutputFormat.setOutputPath(job, new Path(recEngineOutputDir));

        job.setMapperClass(User_PrefGeneratorMapper.class);
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
         **		1.输入路径： 用户偏好向量
         *                  用户阅读过的分类
         *                  用户基础特征信息
         ** 输出：
         **		在线模型需要用的用户总特征向量
         *
         ** 功能描述：
         ** 	在用户偏好向量的基础上拼接前三偏好分类的相似分类，用户阅读过的分类及用户基础特征信息
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        
        //输入文件路径
        String userPrefVector = config.getParam("user_pref_vector", "");
        String userReadClasses = config.getParam("user_read_classes", "");
        String userBaseInfo = config.getParam("user_base_info", "");
             
        //输出路径
        String outputDir = config.getParam("user_vector", "");
        
        job = Job.getInstance(conf, "corelation-personal-recommend");
  
        job.setJarByClass(UserVectorPrepDriver.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(userReadClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(userBaseInfo), TextInputFormat.class, User_BaseInfoMapper.class);
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("userVector_reduce_task_num", 100));
        job.setReducerClass(UserVectorReducer.class);

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

package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.intervectorPrep.InterVectorBetweenUserAndBookReducer;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.intervectorPrep.User_RecommendListMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_PrefVectorMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_ReadClassesMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
public class InterVectorDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new InterVectorDriver(), args);

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
         *                  用户关联推荐列表
         *                  用户阅读过的分类
         ** 输出：
         **		用户对 综合版 图书的打分。
         ** 缓存：
         *      图书雅俗对照表
         *      关联推荐图书库
         ** 功能描述：
         ** 	对用户关联推荐列表里的用户和图书计算用户-图书交叉信息
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        
        String user_recommend_list = config.getParam("user_recommend_his", "/user/recsys/user_read_history");
        String userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector_increment");
        String bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes_increment");
        String outputDir = config.getParam("interVector_result", "/user/hadoop/liujie/test/output/comprehensive_prefrec_result_increment");
        
        String recomBooks = config.getParam("bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/recom_bookinfo");
        String bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
        
        Job job = Job.getInstance(conf, "corelation-personal-recommend");

        FileStatus[] status = FileSystem.get(conf).globStatus(new Path(bookClassifierScores));
        for (int i = 0; i < status.length; i++) {
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());
            log.info("cache file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }
        conf.set("recom.Books", recomBooks);
        status = FileSystem.get(conf).globStatus(new Path(recomBooks));
        for (int i = 0; i < status.length; i++) {
            job.addCacheFile(new Path(status[i].getPath().toString()).toUri());
            log.info("cache file: " + status[i].getPath().toString() + " has been add into distributed cache");
        }

        job.setJarByClass(InterVectorDriver.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(user_recommend_list), TextInputFormat.class, User_RecommendListMapper.class);
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("userVector_reduce_task_num", 100));
        job.setReducerClass(InterVectorBetweenUserAndBookReducer.class);

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

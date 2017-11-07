package com.eb.bi.rs.mras2.unifyrec.UserClassPref;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_PrefVectorMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_ReadClassesMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * Created by LiuJie on 2016/04/10.
 */
public class UserClassPrefDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new UserClassPrefDriver(), args);
        Date dateEnd = new Date();

        long timeCost = dateEnd.getTime() - dateBeg.getTime();

        PluginResult result = PluginUtil.getInstance().getResult();
        result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(ms) :" + timeCost) ;
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
         **		1.输入路径： 用户向量
         **     2.用户阅读过的分类
         ** 输出：
         **	       用户分类偏好表        字段：msisdn用户 |分群|偏执类型|前三分类 |相似分类 |历史阅读分类
         ** 功能描述：
         ** 	构建用户分类偏好表，为咪咕抢发测试使用
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector");
        String bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes");
        String outputDir = config.getParam("user_class_pref", "/user/hadoop/liujie/test/output/user_class_pref");

        Job job = new Job(conf,"user-class-pref");
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

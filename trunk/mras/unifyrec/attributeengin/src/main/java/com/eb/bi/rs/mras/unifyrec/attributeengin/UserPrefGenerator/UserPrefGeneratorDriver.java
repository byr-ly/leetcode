package com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Created by LiMingji on 2015/10/16.
 */
public class UserPrefGeneratorDriver extends Configured implements Tool {

    public static void main( String[] args ) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new UserPrefGeneratorDriver(), args);

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
        String recEngineOutputDir = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector");
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


        Job job = new Job(conf, "user_pref_generator");
        job.setJarByClass(UserPrefGeneratorDriver.class);

        //检查输出目录
        checkOutputPath(recEngineOutputDir);

        FileInputFormat.setInputPaths(job, new Path(prefAllPath));
        FileOutputFormat.setOutputPath(job, new Path(recEngineOutputDir));

        job.setMapperClass(UserPrefGeneratorMapper.class);
        job.setReducerClass(UserPrefGeneratorReducer.class);

        job.setNumReduceTasks(config.getParam("user_pref_generator_reduce_task_num", 10));
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

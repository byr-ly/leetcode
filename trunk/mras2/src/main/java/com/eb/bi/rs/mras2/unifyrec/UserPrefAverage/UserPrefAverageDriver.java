//package com.eb.bi.rs.mras2.unifyrec.UserPrefAverage;
//
//import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
//import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
//import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
//import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
//
//import java.io.IOException;
//import java.net.URI;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
///**
// * Created by LiMingji on 15/10/27.
// */
//public class UserPrefAverageDriver extends Configured implements Tool {
//    public static void main(String[] args) throws Exception {
//        PluginUtil.getInstance().init(args);
//        Logger log = PluginUtil.getInstance().getLogger();
//        Date dateBeg = new Date();
//
//        int ret = ToolRunner.run(new UserPrefAverageDriver(), args);
//        Date dateEnd = new Date();
//
//        long timeCost = dateEnd.getTime() - dateBeg.getTime();
//
//        PluginResult result = PluginUtil.getInstance().getResult();
//        result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
//        result.setParam("timeCosts", timeCost);
//        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
//        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
//        result.save();
//
//        log.info("time cost in total(ms) :" + timeCost) ;
//        System.exit(ret);
//    }
//
//    @Override
//    public int run(String[] strings) throws Exception {
//        Logger log = PluginUtil.getInstance().getLogger();
//        PluginConfig config = PluginUtil.getInstance().getConfig();
//
//        /**
//         * ********************************************************************************************
//         * MAP REDUCE JOB:
//         ** 输入：
//         **		1.输入路径： 用户偏好向量。
//         ** 输出：
//         **		深度用户的偏好向量均值。
//         ** 功能描述：
//         ** 	以深度用户作为用户代表。求用户偏好向量的均值。
//         ** 调度平率：
//         **     每周一次。
//         **
//         *******************************************************************************************/
//
//        log.info("=================================================================================");
//        long start = System.currentTimeMillis();
//        Configuration conf = new Configuration(getConf());
//
//        String prefAllPath = config.getParam("user_pref_all", "/home/recsys/data/recsys_data/attribute_engine/input/user_pref_all");
//        String prefAverageOutputPath = config.getParam("user_pref_average", "/user/hadoop/liujie/test/output/user_pref_average");
//
//        System.out.println("输入路径: " + prefAllPath);
//        System.out.println("输出路径: " + prefAverageOutputPath);
//
//        Job job = new Job(conf, "userpref-avg");
//        job.setJarByClass(UserPrefAverageDriver.class);
//
//        //检查输出目录
//        checkOutputPath(prefAverageOutputPath);
//        FileInputFormat.setInputPaths(job, new Path(prefAllPath));
//        FileOutputFormat.setOutputPath(job, new Path(prefAverageOutputPath));
//
//        job.setMapperClass(UserPrefAverageMapper.class);
//        job.setReducerClass(UserPrefAverageReducer.class);
//
//        job.setNumReduceTasks(config.getParam("userpref_average_reduce_task_num", 1));
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
//        return 0;
//    }
//
//    private void checkOutputPath(String fileName) {
//        Logger log = PluginUtil.getInstance().getLogger();
//        try {
//            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
//            Path path = new Path(fileName);
//            boolean isExists = fs.exists(path);
//            if (isExists) {
//                boolean isDel = fs.delete(path, true);
//                log.info(fileName + "  delete?\t" + isDel);
//            } else {
//                log.info(fileName + "  exist?\t" + isExists);
//            }
//        } catch (IOException e) {
//            log.error(e);
//        }
//    }
//}
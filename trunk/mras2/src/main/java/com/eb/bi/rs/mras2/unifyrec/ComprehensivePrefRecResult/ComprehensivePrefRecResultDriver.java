//package com.eb.bi.rs.mras2.unifyrec.ComprehensivePrefRecResult;
//
//
//import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
//import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
//import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
//import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_PrefVectorMapper;
//import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_ReadClassesMapper;
//import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_ReadHistoryMapper;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
// * Created by LiuJie on 2016/04/10.
// */
//public class ComprehensivePrefRecResultDriver extends Configured implements Tool{
//
//    public static void main(String[] args) throws Exception {
//        PluginUtil.getInstance().init(args);
//        Logger log = PluginUtil.getInstance().getLogger();
//        Date dateBeg = new Date();
//
//        int ret = ToolRunner.run(new ComprehensivePrefRecResultDriver(), args);
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
//         **     2.用户阅读过的分类
//         ** 输出：
//         **		用户对图书的打分。
//         ** 缓存：
//         *      图书雅俗对照表
//         *      推荐图书库
//         ** 功能描述：
//         ** 	求用户对推荐图书的打分
//         **
//         *******************************************************************************************/
//
//        log.info("=================================================================================");
//        long start = System.currentTimeMillis();
//        Configuration conf = new Configuration(getConf());
//
//        String userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector");
//        String bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes");
//        String outputDir = config.getParam("comprehensive_prefrec_result", "/user/hadoop/liujie/test/output/comprehensive_prefrec_result");
//        String bookClassifierScores = config.getParam("classifier_output_d", "/home/recsys/data/recsys_data/attribute_engine/input/classifier");
//        String recomBooks = config.getParam("recom_bookinfo", "/home/recsys/data/recsys_data/attribute_engine/input/recom_bookinfo");
//        String userReadHistory = config.getParam("user_read_history", "/user/hadoop/liujie/test/output/user_read_history");
//
//        String isDug = config.getParam("comprehensive_prefrec_result_reducer_debug", "false");
//        conf.setBoolean("comprehensive_prefrec_result_reducer_debug", Boolean.parseBoolean(isDug));
//
//        System.out.println("雅俗表信息加载到DistributedCache");
//        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(bookClassifierScores));
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }
//
//        System.out.println("推荐图书表加载到DistributedCache");
//        fs = FileSystem.get(conf).listStatus(new Path(recomBooks));
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }
//
//        conf.set("comprehensive_prefrec_result_reduce_top", config.getParam("comprehensive_prefrec_result_reduce_top", "2000"));
//
//        Job job = new Job(conf, "comprehensive_pref_recommend");
//        job.setJarByClass(ComprehensivePrefRecResultDriver.class);
//
//        //输入目录
//        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
//        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
//        MultipleInputs.addInputPath(job, new Path(userReadHistory), TextInputFormat.class, User_ReadHistoryMapper.class);
//
//        //输出目录
//        checkOutputPath(outputDir);
//        FileOutputFormat.setOutputPath(job, new Path(outputDir));
//
//        job.setNumReduceTasks(config.getParam("comprehensive_prefrec_result_reduce_task_num", 100));
//        job.setReducerClass(ComprehensivePrefRecResultReducer.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
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

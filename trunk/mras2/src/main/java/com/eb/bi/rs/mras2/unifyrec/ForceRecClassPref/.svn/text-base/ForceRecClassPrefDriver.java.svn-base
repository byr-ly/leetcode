//package com.eb.bi.rs.mras2.unifyrec.ForceRecClassPref;
//
//import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
//import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
//import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
//import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_PrefVectorMapper;
//import com.eb.bi.rs.mras.unifyrec.attributeengin.UserBookScoreTools.User_ReadClassesMapper;
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
//public class ForceRecClassPrefDriver extends Configured implements Tool{
//
//    public static void main(String[] args) throws Exception {
//        PluginUtil.getInstance().init(args);
//        Logger log = PluginUtil.getInstance().getLogger();
//        Date dateBeg = new Date();
//
//        int ret = ToolRunner.run(new ForceRecClassPrefDriver(), args);
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
//         **		1.输入路径： 用户历史偏好。
//         **     2.用户阅读过的分类
//         ** 输出：
//         **		用户对图书的分类偏好打分。
//         ** 缓存：
//         *      各个分版的编辑强推图书
//         ** 功能描述：
//         ** 	求用户对编辑强推图书的分类偏好打分
//         **
//         *******************************************************************************************/
//
//        log.info("=================================================================================");
//        long start = System.currentTimeMillis();
//        Configuration conf = new Configuration(getConf());
//
//        String userPrefVector = config.getParam("user_pref_vector", "/user/hadoop/liujie/test/output/user_pref_vector");
//        String bookClasses = config.getParam("user_read_classes", "/user/hadoop/liujie/test/output/user_read_classes");
//        String outputDir = config.getParam("forcerec_classpref_result", "/home/recsys/data/recsys_data/output/forcerec_classpref_result");
//        String manRecomBooks = config.getParam("man_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/man_force_recom_book");
//        String femaleRecomBooks = config.getParam("female_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/female_force_recom_book");
//        String publishRecomBooks = config.getParam("publish_force_recom_book", "/home/recsys/data/recsys_data/attribute_engine/input/publish_force_recom_book");
//
//        conf.set("forcerec_classpref_reduce_class_limit", config.getParam("forcerec_classpref_reduce_class_limit", "0.6"));
//
//        System.out.println("男版编辑强推库图书信息加载到DistributedCache");
//        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path(manRecomBooks));
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }
//
//        System.out.println("女版编辑强推库图书信息加载到DistributedCache");
//        fs = FileSystem.get(conf).listStatus(new Path(femaleRecomBooks));
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }
//        System.out.println("出版编辑强推库图书信息加载到DistributedCache");
//        fs = FileSystem.get(conf).listStatus(new Path(publishRecomBooks));
//        for (int i = 0; i < fs.length; i++) {
//            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
//            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
//        }
//        Job job = new Job(conf, "force-rec-class-pref");
//        job.setJarByClass(ForceRecClassPrefDriver.class);
//
//        //输入目录
//        MultipleInputs.addInputPath(job, new Path(userPrefVector), TextInputFormat.class, User_PrefVectorMapper.class);
//        MultipleInputs.addInputPath(job, new Path(bookClasses), TextInputFormat.class, User_ReadClassesMapper.class);
//        //输出目录
//        checkOutputPath(outputDir);
//        FileOutputFormat.setOutputPath(job, new Path(outputDir));
//
//        job.setNumReduceTasks(config.getParam("forcerec_classpref_reduce_task_num", 60));
//        job.setReducerClass(ForceRecClassPrefReducer.class);
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

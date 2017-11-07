package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo.getBookMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo.getRecClickMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookInfo.getUserBookReducer;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick.getClickMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick.getRecClickReducer;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.recClick.getRecListMapper;
import com.eb.bi.rs.mras2.new_correlation.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**拼接训练数据信息
 * Created by linwanying on 2017/6/20.
 */
public class jointUBDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public jointUBDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new jointUBDriver(args), args);

        Date end = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String endTime = format.format(end);
        long timeCost = end.getTime() - begin.getTime();

        PluginResult result = pluginUtil.getResult();
        result.setParam("endTime", endTime);
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(s): " + (timeCost / 1000.0));
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        PluginConfig config = pluginUtil.getConfig();
        Configuration conf;
        Job job;
        FileStatus[] fs;
        long start;

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户推荐日志，用户点击结果。
         ** 输出：
         **		用户当天推荐图书的所有点击结果。
         **
         **
         *******************************************************************************************/

        log.info("start to get log rec result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "getRecClick");

        String clickInputPath = config.getParam("correlationrec_click_path", "/user/ebupt/linwanying/correlationrec/click");
        String clickleafinput = config.getParam("correlationrec_click_leaf_path", "/user_correlation_rec_click_books");
        String recClickoutputDir = config.getParam("correlationrec_recClick_path", "/user/ebupt/linwanying/correlationrec/recclick");
        String logInputPath = config.getParam("correlationrec_log_path", "/user/ebupt/linwanying/correlationrec/log");
        log.info("correlationrec_trainingdata_time:"+config.getParam("correlationrec_trainingdata_time", "2592000000"));
//        System.out.println(Long.parseLong(config.getParam("correlationrec_trainingdata_time", "2592000000")));
        long timerange = Long.parseLong(config.getParam("correlationrec_trainingdata_time", "2592000000"));
        StringBuilder clickpaths = new StringBuilder();
        FileStatus[] fsinput = FileSystem.get(conf).globStatus(new Path(clickInputPath + "/*"));
        for (FileStatus status : fsinput) {
            if (TimeUtil.getTimeMillis(TimeUtil.getToday("yyyyMMdd"), "yyyyMMdd") - TimeUtil.getTimeMillis(status.getPath().getName(), "yyyyMMdd") <= timerange) {
//                log.info(status.getPath().getName());
                clickpaths.append(clickInputPath).append("/").append(status.getPath().getName()).append(clickleafinput).append(",");
                MultipleInputs.addInputPath(job, new Path(clickInputPath+"/"+status.getPath().getName()+clickleafinput), TextInputFormat.class, getClickMapper.class);
            }
        }
        log.info("clickpaths:"+clickpaths.toString());

        StringBuilder logInputpaths = new StringBuilder();
        fsinput = FileSystem.get(conf).globStatus(new Path(logInputPath + "/*"));
        for (FileStatus status : fsinput) {
            if (TimeUtil.getTimeMillis(TimeUtil.getToday("yyyy-MM-dd"), "yyyy-MM-dd") - TimeUtil.getTimeMillis(status.getPath().getName(), "yyyy-MM-dd") <= timerange) {
//                log.info(status.getPath().getName());
                logInputpaths.append(logInputPath).append("/").append(status.getPath().getName()).append(",");
                MultipleInputs.addInputPath(job, new Path(logInputPath+"/"+status.getPath().getName()), TextInputFormat.class, getRecListMapper.class);
            }
        }
        log.info("logInputPath:"+logInputpaths.toString());

        job.setJarByClass(jointUBDriver.class);
//        //M-R
//        MultipleInputs.addInputPath(job, new Path(clickpaths.toString()), TextInputFormat.class, getClickMapper.class);
//        MultipleInputs.addInputPath(job, new Path(logInputpaths.toString()), TextInputFormat.class, getRecListMapper.class);
        //设置输出地址
        check(recClickoutputDir);
        FileOutputFormat.setOutputPath(job, new Path(recClickoutputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.rec.click.reduce.num", "5")));
        job.setReducerClass(getRecClickReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job getRecClick complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job getRecClick failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

//        /**
//         * ********************************************************************************************
//         * MAP REDUCE JOB:
//         ** 输入：
//         **		输入路径：用户当天推荐图书的所有点击结果，用户信息。
//         ** 输出：
//         **		用户当天推荐图书的所有点击结果+用户信息。
//         **
//         **
//         *******************************************************************************************/
//
//        log.info("start to joint user result...");
//        start = System.currentTimeMillis();
//        conf = new Configuration(getConf());
//        job = Job.getInstance(conf, "jointUser");
//        job.setJarByClass(jointUBDriver.class);
//
//        String jointUseroutputDir = config.getParam("correlationrec_jointUser_path", "/user/ebupt/linwanying/correlationrec/jointUser");
//        String userInfoPath = config.getParam("correlationrec_userInfo_path", "/user/ebupt/linwanying/correlationrec/user");
//
//        //M-R
//        MultipleInputs.addInputPath(job, new Path(recClickoutputDir), TextInputFormat.class, getRecClickMapper.class);
//        MultipleInputs.addInputPath(job, new Path(userInfoPath), TextInputFormat.class, getUserMapper.class);
//        //设置输出地址
//        check(jointUseroutputDir);
//        FileOutputFormat.setOutputPath(job, new Path(jointUseroutputDir));
//
//        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.joint.user.reduce.num", "5")));
//        job.setReducerClass(jointUserReducer.class);
//        //设置输出类型(map)
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        //设置输入/输出格式
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        if (job.waitForCompletion(true)) {
//            log.info("generate job jointUser complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
//        } else {
//            log.error("generate job jointUser failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
//            return 1;
//        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户当天推荐图书的所有点击结果+用户信息，图书信息。
         ** 输出：
         **		用户当天推荐图书的所有点击结果+用户信息+图书信息。
         **
         **
         *******************************************************************************************/

        log.info("start to joint book result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "jointBook");
        job.setJarByClass(jointUBDriver.class);

        String jointBookoutputDir = config.getParam("correlationrec_jointBook_path", "/user/ebupt/linwanying/correlationrec/jointBook");
        String bookInfoPath = config.getParam("correlationrec_bookInfo_path", "/user/ebupt/linwanying/correlationrec/book");

        //M-R
        MultipleInputs.addInputPath(job, new Path(recClickoutputDir), TextInputFormat.class, getRecClickMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookInfoPath), TextInputFormat.class, getBookMapper.class);
        //设置输出地址
        check(jointBookoutputDir);
        FileOutputFormat.setOutputPath(job, new Path(jointBookoutputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.joint.book.reduce.num", "5")));
        job.setReducerClass(getUserBookReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job jointBook complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job jointBook failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        return 0;
    }

    private void check(String fileName) {
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {    //if exists, delete
                boolean isDel = fs.delete(f, true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

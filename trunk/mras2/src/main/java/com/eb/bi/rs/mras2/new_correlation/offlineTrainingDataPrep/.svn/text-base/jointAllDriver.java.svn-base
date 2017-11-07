package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo.getUserBookMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo.getUserBookinfoMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUBinfo.jointUBinfoReducer;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.getAndJoinUserInfo.*;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo.getBookCorreMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo.getUBinfoMapper;
import com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.joinBookCorreInfo.jointFinalReducer;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**拼接用户图书关联信息和图书之间关联信息
 * Created by linwanying on 2017/6/21.
 */
public class jointAllDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public jointAllDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new jointAllDriver(args), args);

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
         **		输入路径：用户当天推荐图书的所有点击结果+图书信息，用户图书交叉信息。
         ** 输出：
         **		用户当天推荐图书的所有点击结果+图书信息+用户图书交叉信息。
         **
         **
         *******************************************************************************************/

        log.info("start to get user book info result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "getUBinfo");
        job.setJarByClass(jointAllDriver.class);

        String jointBookInputPath = config.getParam("correlationrec_jointBook_path", "/user/ebupt/linwanying/correlationrec/jointBook");
        String UBinfooutputDir = config.getParam("correlationrec_UBinfoCorre_path", "/user/recsys/linwanying/correlationrec/jointUserBookInfo");
        String UBinfoInputPath = config.getParam("correlationrec_UBinfo_path", "/user/recsys/liujie/interVector/output/interVector_result");

        //M-R
        MultipleInputs.addInputPath(job, new Path(jointBookInputPath), TextInputFormat.class, getUserBookMapper.class);
        MultipleInputs.addInputPath(job, new Path(UBinfoInputPath), TextInputFormat.class, getUserBookinfoMapper.class);
        //设置输出地址
        check(UBinfooutputDir);
        FileOutputFormat.setOutputPath(job, new Path(UBinfooutputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.userBookInfo.reduce.num", "5")));
        job.setReducerClass(jointUBinfoReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job getUBinfo complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job getUBinfo failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用用户当天推荐图书的所有点击结果+图书信息+用户图书交叉信息，图书关联信息。
         ** 输出：
         **		用用户当天推荐图书的所有点击结果+图书信息+用户图书交叉信息+图书关联信息。
         **
         **
         *******************************************************************************************/

        log.info("start to joint bookCorrelation result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "jointBookCorre");
        job.setJarByClass(jointAllDriver.class);

        String jointAlloutputDir = config.getParam("correlationrec_jointAll_path", "/user/recsys/linwanying/correlationrec/jointAll");
        String bookCorrePath = config.getParam("correlationrec_userInfo_path", "/user/recsys/bookrec/corelation_rec/read/service/read_filtered_indicator_path/");

        //M-R
        MultipleInputs.addInputPath(job, new Path(UBinfooutputDir), TextInputFormat.class, getUBinfoMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookCorrePath), TextInputFormat.class, getBookCorreMapper.class);
        //设置输出地址
        check(jointAlloutputDir);
        FileOutputFormat.setOutputPath(job, new Path(jointAlloutputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.joint.all.reduce.num", "5")));
        job.setReducerClass(jointFinalReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job bookCorrelation complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job bookCorrelation failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户近6月阅读情况。
         ** 输出：
         **		用户近一周和近一月的下载，点击，订购情况。
         **
         **
         *******************************************************************************************/

        log.info("start to Statistics user reading result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        conf.set("date", config.getParam("date", "20170602"));
        job = Job.getInstance(conf, "getUserStatistics");
        job.setJarByClass(jointAllDriver.class);

        String getUserStatisticsoutputDir = config.getParam("correlationrec_getUserStatistics_path", "/user/recsys/linwanying/correlationrec/getUserStatistics");
        String userStatisticsPath = config.getParam("correlationrec_userStatistics_path", "/user/recsys/common/user_read_books_6cm");

        //M-R
        job.setMapperClass(getUserActionMapper.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(userStatisticsPath));
        //设置输出地址
        check(getUserStatisticsoutputDir);
        FileOutputFormat.setOutputPath(job, new Path(getUserStatisticsoutputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.getUserStatistics.reduce.num", "5")));
        job.setReducerClass(getUserActionReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job getUserStatistics complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job getUserStatistics failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户当天推荐图书的所有点击结果+图书信息+用户图书交叉信息+图书关联信息,用户统计信息。
         ** 输出：
         **		用户当天推荐图书的所有点击结果+图书信息+用户图书交叉信息+图书关联信息+用户统计信息。
         **
         **
         *******************************************************************************************/

        log.info("start to joint user statistics result...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "jointUserStatistics");
        job.setJarByClass(jointAllDriver.class);

        String finaloutput = config.getParam("correlationrec_finaloutput_path", "/user/recsys/linwanying/correlationrec/jointUserStatistics");

        //M-R
        MultipleInputs.addInputPath(job, new Path(getUserStatisticsoutputDir), TextInputFormat.class, getUserStatisticsMapper.class);
        MultipleInputs.addInputPath(job, new Path(jointAlloutputDir), TextInputFormat.class, getInfoMapper.class);
        //设置输出地址
        check(finaloutput);
        FileOutputFormat.setOutputPath(job, new Path(finaloutput));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("correlationrec.joint.user.statistics.reduce.num", "5")));
        job.setReducerClass(jointUserStatisReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job jointUserStatistics complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job jointUserStatistics failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
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

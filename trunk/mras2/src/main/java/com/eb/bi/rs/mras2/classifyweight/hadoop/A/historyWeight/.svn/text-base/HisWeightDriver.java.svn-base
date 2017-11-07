package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight;

/**计算用户历史分类权重
 * Created by linwanying on 2017/3/20.
 */

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

/**计算历史权重
 * Created by linwanying on 2017/3/20.
 */
public class HisWeightDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public HisWeightDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new HisWeightDriver(args), args);

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
         **		输入路径：用户图书打分表。
         ** addCache: 图书信息表。
         ** 输出：
         **		用户对各分类的偏好程度。
         **
         **
         *******************************************************************************************/

        log.info("start to compute the history class weight...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        conf.set("filter_time", config.getParam("filter_time", "1555200000"));

        FileStatus[] fsinput;
        String inputPath = config.getParam("user_book_score_path", "/user/recsys/markrelation/bookmark/newuserbooklastday_back/");
        String ucoutputDir = config.getParam("user_class_score_path", "linwanying/classifyweight/middle/user_history");
        String bookClassInfo = config.getParam("book_class_path", "/user/idox/newunifyrec/base_score");

        boolean flag = false;
        fsinput = FileSystem.get(conf).globStatus(new Path(inputPath + "/*"));
        for (FileStatus status : fsinput) {
            log.info(status.getPath().getName());
            if (status.getPath().getName().contains(TimeUtil.getYesterdayDate())) {
                inputPath = inputPath + "/" + TimeUtil.getYesterdayDate();
                flag = true;
            }
        }
        if (!flag) return 0;
        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + ucoutputDir);
        System.out.println("图书信息路径: " + bookClassInfo);

        job = Job.getInstance(conf, "userClassScore");
        fs = FileSystem.get(conf).globStatus(new Path(bookClassInfo));
        for (FileStatus st : fs) {
            job.addCacheFile(st.getPath().toUri());
            log.info("terminal file: " + st.getPath().toUri() + " has been add into distributed cache");
        }
        job.setJarByClass(HisWeightDriver.class);

        //M-R
        job.setMapperClass(UserClassMapper.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("classifyweightA.reduce.num.userclass", "5")));
        job.setReducerClass(UserClassReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        //设置输出地址
        FileOutputFormat.setOutputPath(job, new Path(ucoutputDir));
        check(ucoutputDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job userclass complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job userclass failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户对各分类的偏好。
         ** 输出：
         **		用户对应所有分类总和。
         **
         **
         *******************************************************************************************/
        log.info("start to compute the sum of each user...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String sumoutputDir = config.getParam("user_sum_path", "linwanying/classifyweight/middle/user_sum");

        System.out.println("输出路径: " + sumoutputDir);

        job = Job.getInstance(conf, "ClassSum");
        job.setJarByClass(HisWeightDriver.class);

        //M-R
        job.setMapperClass(ClassSumMapper.class);
//        job.setCombinerClass(ClassSumReducer.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("classifyweightA.reduce.num.classsum", "5")));
        job.setReducerClass(ClassSumReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(ucoutputDir));
        //设置输出地址
        FileOutputFormat.setOutputPath(job, new Path(sumoutputDir));
        check(sumoutputDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job ClassSum complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job ClassSum failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户对分类的偏好。
         *      addCache: 用户个分类得分的总和
         ** 输出：
         **		用户历史分类权重归一化结果。
         **
         **
         *******************************************************************************************/
        log.info("start to compute the normalization of classify weight...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String outputDir = config.getParam("user_class_normalize_path", "linwanying/classifyweight/result/user_class_normalize");

        System.out.println("输出路径: " + outputDir);

        job = Job.getInstance(conf, "Normalization");
//        fs = FileSystem.get(conf).globStatus(new Path(sumoutputDir + "/part-*"));
//        for (FileStatus st : fs) {
//            job.addCacheFile(st.getPath().toUri());
//            log.info("terminal file: " + st.getPath().toUri() + " has been add into distributed cache");
//        }
        job.setJarByClass(HisWeightDriver.class);
        System.out.println("输入1路径: " + ucoutputDir);
        //M-R
        MultipleInputs.addInputPath(job, new Path(sumoutputDir), TextInputFormat.class, UCScoreMapper.class);
        MultipleInputs.addInputPath(job, new Path(ucoutputDir), TextInputFormat.class, UserSumMapper.class);
        //设置输出地址
        check(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(Integer.parseInt(config.getParam("classifyweightA.reduce.num.normalize", "0")));
        job.setReducerClass(NormalizeReducer.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输入/输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            log.info("generate job Normalization complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job Normalization failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        return 0;
    }

    public void check(String fileName) {
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


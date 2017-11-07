package com.eb.bi.rs.mras2.consonance;

import com.eb.bi.rs.frame2.algorithm.dataPreprocessing.theEntropyMethod.*;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

/**the entropy method
 * Created by linwanying on 2016/12/2.
 */
public class EntropyMethodDriver extends Configured implements Tool {

    private static PluginUtil pluginUtil;
    private static Logger log;

    public EntropyMethodDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new EntropyMethodDriver(args), args);

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
        Logger log = Logger.getLogger("EntropyMethodDriver");
        PluginConfig config = pluginUtil.getConfig();
        Configuration conf;
        Job job;
        FileStatus[] status;
        long start;

        //最大最小值job配置
        log.info("========start to run peak job========");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        conf.set("action_num", config.getParam("action_num", ""));
        conf.set("separator", config.getParam("separator", ""));
        job = Job.getInstance(conf, "peak");
        job.setJarByClass(EntropyMethodDriver.class);
        //M-R
        job.setMapperClass(PeakMapper.class);
        job.setCombinerClass(PeakReducer.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("entropy.reduce.num.peak", "5")));
        job.setReducerClass(PeakReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(config.getParam("entropy.originalinfo", "/user/ebupt/linwanying/product/entropy/originalinfo")));
        //设置输出地址
        String peakDir = config.getParam("entropy.peak", "/user/ebupt/linwanying/product/entropy/peak");
        FileOutputFormat.setOutputPath(job, new Path(peakDir));
        check(peakDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job peak complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job peak failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        //标准化job配置
        log.info("========start to run standardize job========");
        conf = new Configuration(getConf());
        conf.set("action_num", config.getParam("action_num", ""));
        conf.set("standardize_type", config.getParam("standardize_type", ""));
        job = Job.getInstance(conf, "standardize");
        status = FileSystem.get(conf).globStatus(new Path(peakDir + "/part-*"));
        for (FileStatus st : status) {
            job.addCacheFile(st.getPath().toUri());
            log.info("terminal file: " + st.getPath().toUri() + " has been add into distributed cache");
        }

        job.setJarByClass(EntropyMethodDriver.class);
        //M-R
        job.setMapperClass(StandardizeMapper.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("entropy.reduce.num.standardize", "5")));
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(config.getParam("entropy.originalinfo", "/user/ebupt/linwanying/product/entropy/originalinfo")));
        //设置输出地址
        String standardizeDir = config.getParam("entropy.standardize", "/user/ebupt/linwanying/product/entropy/standardize");
        FileOutputFormat.setOutputPath(job, new Path(standardizeDir));
        check(standardizeDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job standardize complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job standardize failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        //用户行为求和job配置
        log.info("========start to run sum job========");
        conf = new Configuration(getConf());
        conf.set("action_num", config.getParam("action_num", ""));
        job = Job.getInstance(conf, "sum");
        job.setJarByClass(EntropyMethodDriver.class);
        //M-R
        job.setMapperClass(SumMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("entropy.reduce.num.sum", "5")));
        job.setReducerClass(SumReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(standardizeDir));
        //设置输出地址
        String sumDir = config.getParam("entropy.sum", "/user/ebupt/linwanying/product/entropy/sum");
        FileOutputFormat.setOutputPath(job, new Path(sumDir));
        check(sumDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job sum complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job sum failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        //用户行为求熵值job配置
        log.info("========start to run entropy job========");
        conf = new Configuration(getConf());
        conf.set("action_num", config.getParam("action_num", ""));
        job = Job.getInstance(conf, "entropy");
        status = FileSystem.get(conf).globStatus(new Path(sumDir + "/part-*"));
        for (FileStatus st : status) {
            job.addCacheFile(st.getPath().toUri());
            log.info("terminal file: " + st.getPath().toUri() + " has been add into distributed cache");
        }
        job.setJarByClass(EntropyMethodDriver.class);
        //M-R
        job.setMapperClass(SumMapper.class);
        job.setCombinerClass(EntropyReducer.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("entropy.reduce.num.entropy", "5")));
        job.setReducerClass(EntropyReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(standardizeDir));
        //设置输出地址
        String entropyDir = config.getParam("entropy.entropy", "/user/ebupt/linwanying/product/entropy/entropy");
        FileOutputFormat.setOutputPath(job, new Path(entropyDir));
        check(entropyDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job entropy complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job entropy failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }

        //用户品牌得分job配置
        log.info("========start to run score job========");
        conf = new Configuration(getConf());
        conf.set("action_num", config.getParam("action_num", ""));
        job = Job.getInstance(conf, "score");
        status = FileSystem.get(conf).globStatus(new Path(entropyDir + "/part-*"));
        for (FileStatus st : status) {
            job.addCacheFile(st.getPath().toUri());
            log.info("terminal file: " + st.getPath().toUri() + " has been add into distributed cache");
        }
        job.setJarByClass(EntropyMethodDriver.class);
        //M-R
        job.setMapperClass(ScoreMapper.class);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("entropy.reduce.num.score", "5")));
        job.setReducerClass(ScoreReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(standardizeDir));
        //设置输出地址
        String scoreDir = config.getParam("entropy.score", "/user/ebupt/linwanying/product/entropy/score");
        FileOutputFormat.setOutputPath(job, new Path(scoreDir));
        check(scoreDir);

        if (job.waitForCompletion(true)) {
            log.info("generate user score complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate user score failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
            return 1;
        }
        return 0;
    }

    public void check(String fileName) {
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {	//if exists, delete
                boolean isDel = fs.delete(f,true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package com.eb.bi.rs.mras.generec.hadoop;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras.generec.hadoop.category_pref.CategoryPrefMapper;
import com.eb.bi.rs.mras.generec.hadoop.category_pref.CategoryPrefReducer;
import com.eb.bi.rs.mras.generec.hadoop.gene_pref.GenePrefMapper;
import com.eb.bi.rs.mras.generec.hadoop.gene_pref.GenePrefReducer;
import com.eb.bi.rs.mras.generec.hadoop.pref_normalized.PrefNormalizedMapper;
import com.eb.bi.rs.mras.generec.hadoop.pref_normalized.PrefNormalizedReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

/**
 * Created by liyang on 2016/6/27.
 */
public class GeneRecomDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public GeneRecomDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new GeneRecomDriver(args), args);

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
         **		输入路径：用户历史偏好表。
         **     用户分类偏好表。
         ** 输出：
         **		用户对各分类的偏好程度。
         **
         **
         *******************************************************************************************/

        log.info("start to compute the score of type...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String inputPath = config.getParam("user_input_path", "liyang/generec/input");
        String recOutputDir = config.getParam("type_pref", "liyang/generec/middle/type_pref");
        String prefBookInfo = config.getParam("user_type_cache_path", "liyang/generec/cache/usertype/usertype");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("用户分类信息路径: " + prefBookInfo);

        job = new Job(conf);
        job.setJarByClass(GeneRecomDriver.class);
        job.setJobName("CategoryPref");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CategoryPrefMapper.class);
        job.setReducerClass(CategoryPrefReducer.class);

        job.setNumReduceTasks(config.getParam("category_pref_reduce_task_num", 10));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, CategoryPrefMapper.class);
        MultipleInputs.addInputPath(job, new Path(prefBookInfo),TextInputFormat.class,CategoryPrefMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径：用户分类偏好表。
         ** 缓存：
         **     基因分类表。
         ** 输出：
         **		用户对各基因的偏好程度。
         **
         **
         *******************************************************************************************/

        log.info("start to compute the score of gene...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        inputPath = config.getParam("type_pref", "liyang/generec/middle/type_pref");
        recOutputDir = config.getParam("gene_pref", "liyang/generec/middle/gene_pref");
        prefBookInfo = config.getParam("gene_type_cache_path", "liyang/generec/cache/genetype/genetype");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("基因分类信息路径: " + prefBookInfo);

        //用户分类信息
        fs = FileSystem.get(conf).globStatus(new Path(prefBookInfo));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf);
        job.setJarByClass(GeneRecomDriver.class);
        job.setJobName("GenePref");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(GenePrefMapper.class);
        job.setReducerClass(GenePrefReducer.class);

        job.setNumReduceTasks(config.getParam("gene_pref_reduce_task_num", 10));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径：用户基因偏好表。
         ** 输出：
         **		用户对各基因的归一化偏好程度。
         **
         **
         *******************************************************************************************/

        log.info("start to normalize the score of gene...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        inputPath = config.getParam("gene_pref", "liyang/generec/middle/gene_pref");
        recOutputDir = config.getParam("gene_normalize", "liyang/generec/output/gene_normalize");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);

        job = new Job(conf);
        job.setJarByClass(GeneRecomDriver.class);
        job.setJobName("PrefNormalized");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(PrefNormalizedMapper.class);
        job.setReducerClass(PrefNormalizedReducer.class);

        job.setNumReduceTasks(config.getParam("pref_normalized_reduce_task_num", 10));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
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


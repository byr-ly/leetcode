package com.eb.bi.rs.mras.unifyrec.attributeengin.PrefRecResultMerge;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

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
public class PrefRecResultMergeDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new PrefRecResultMergeDriver(), args);
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
         **		1.输入路径： 综合版及分版偏好推荐结果。
         ** 输出：
         **		去重后的推荐结果合集。
         ** 功能描述：
         ** 	求出各个版推荐结果的合集。
         **
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

        String inputPath1 = config.getParam("comprehensive_prefrec_result", "/user/hadoop/liujie/test/output/comprehensive_prefrec_result");
        String inputPath2 = config.getParam("man_prefrec_result", "/user/hadoop/liujie/test/output/man_prefrec_result");
        String inputPath3 = config.getParam("female_prefrec_result", "/user/hadoop/liujie/test/output/female_prefrec_result");
        String inputPath4 = config.getParam("publish_prefrec_result", "/user/hadoop/liujie/test/output/publish_prefrec_result");
        
        String outputPath = config.getParam("prefrec_merge_result", "/user/hadoop/liujie/test/output/prefrec_merge_result");

        System.out.println("输入路径1: " + inputPath1);
        System.out.println("输入路径2: " + inputPath2);
        System.out.println("输入路径3: " + inputPath3);
        System.out.println("输入路径4: " + inputPath4);
        System.out.println("输出路径: " + outputPath);

        Job job = new Job(conf, "prefrec-result-merge");
        job.setJarByClass(PrefRecResultMergeDriver.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath3), TextInputFormat.class, PrefRecResultMergeMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath4), TextInputFormat.class, PrefRecResultMergeMapper.class);
        //输出目录
        checkOutputPath(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setReducerClass(PrefRecResultMergeReducer.class);
        job.setNumReduceTasks(config.getParam("prerec_result_merge_reduce_task_num", 200));

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

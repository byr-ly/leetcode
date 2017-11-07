package com.eb.bi.rs.mras.andnewsrec.compare_result;

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
 * Created by LiMingji on 2016/4/18.
 */
public class CompareResultDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new CompareResultDriver(), args);

        Date dateEnd = new Date();
        long timeCost = dateEnd.getTime() - dateBeg.getTime();

        PluginResult result = PluginUtil.getInstance().getResult();
        result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(ms) :" + timeCost);
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
         **		词|分类   相似新闻，相似的分 ...
         ** 输出：
         **		把结果有变动的推荐结果写入HBase和Kafka
         **
         *******************************************************************************************/

        log.info("=================================================================================");

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        String compareResultInputPathToday = config.getParam("simi_ret_output", "limingji/andnews/output/rets_output");
        String compareResultInputPathYesterday = config.getParam("simi_ret_output_yesterday", "limingji/andnews/output/rets_output_yesterday");
        String compareResultOutputPath = config.getParam("compare_ret_output", "limingji/andnews/output/compare_ret_output");

        Job job = new Job(conf, "CompareResult");
        job.setJarByClass(CompareResultDriver.class);

        System.out.println("输入路径: " + compareResultInputPathToday);
        System.out.println("输入路径: " + compareResultInputPathYesterday);
        System.out.println("输出路径: " + compareResultOutputPath);

        checkOutputPath(compareResultOutputPath);
        MultipleInputs.addInputPath(job, new Path(compareResultInputPathToday), TextInputFormat.class, CompareResultMapperToday.class);
        MultipleInputs.addInputPath(job, new Path(compareResultInputPathYesterday), TextInputFormat.class, CompareResultMapperYesterday.class);

        FileOutputFormat.setOutputPath(job, new Path(compareResultOutputPath));
        job.setReducerClass(CompareResultReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(config.getParam("compare_result_task_num", 10));
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

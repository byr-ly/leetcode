package com.eb.bi.rs.mras2.andnewsrec.get_keyword_news;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LiMingji on 2016/4/12.
 */
public class GetKeyWordNewsDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new GetKeyWordNewsDriver(), args);

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
         **		新闻-词-权重  newsID|classID  word,weight|word,weight|...
         ** 输出：
         **		词, 新闻-权重  word|classID, newsID,weight
         ** 功能描述：
         ** 	转置上述矩阵。
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        Job job = new Job(conf, "TransposeOnly");
        job.setJarByClass(GetKeyWordNewsDriver.class);

        String transpose_only_input = config.getParam("word_weight_output", "limingji/andnews/output/news_word_transpose");
        String transpose_only_output = config.getParam("transpose_only_output", "limingji/andnews/output/news_word_transpose");

        //检查输出目录
        checkOutputPath(transpose_only_output);
        System.out.println("输入路径: " + transpose_only_input);
        System.out.println("输出路径: " + transpose_only_output);

        FileInputFormat.addInputPath(job, new Path(transpose_only_input));
        FileOutputFormat.setOutputPath(job, new Path(transpose_only_output));

        job.setMapperClass(GetKeyWordNewsMapper.class);
        job.setReducerClass(GetKeyWordNewsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(config.getParam("transpose_only_reduce_task_num", 10));

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        return 0;
    }

    private void checkOutputPath(String fileName) {notifyAll();
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


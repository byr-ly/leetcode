package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.bookvecorPrep.BookVectorReducer;
import com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep.User_BaseInfoMapper;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.User_ReadClassesMapper;
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
 * Created by LiuJie on 2017/08/03.
 */
public class BookVectorPrepDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        PluginUtil.getInstance().init(args);
        Logger log = PluginUtil.getInstance().getLogger();
        Date dateBeg = new Date();

        int ret = ToolRunner.run(new BookVectorPrepDriver(), args);

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
         **		1.输入路径： 图书基础特征信息
         *                  图书雅俗分表
         ** 输出：
         **		在线模型需要用的图书特征
         *
         ** 功能描述：
         ** 	在图书基础特征上拼接图书雅俗分
         **
         *******************************************************************************************/

        log.info("=================================================================================");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        
        //输入文件路径
        String bookEleganceAndVugarityScores = config.getParam("book_elegance_vugarity_scores", "");
        String bookBaseInfo = config.getParam("book_base_info", "");
             
        //输出路径
        String outputDir = config.getParam("book_vector", "");
        
        Job job = Job.getInstance(conf, "corelation-personal-recommend");
  
        job.setJarByClass(BookVectorPrepDriver.class);

        //输入目录
        MultipleInputs.addInputPath(job, new Path(bookEleganceAndVugarityScores), TextInputFormat.class, User_ReadClassesMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookBaseInfo), TextInputFormat.class, User_BaseInfoMapper.class);
        //输出目录
        checkOutputPath(outputDir);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setNumReduceTasks(config.getParam("bookVector_reduce_task_num", 10));
        job.setReducerClass(BookVectorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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

package com.eb.bi.rs.mras.authorrec.itemcf.module_test;

/**
 * Created by LiMingji on 2015/12/15.
 */

import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.readscore.ReadAuthorScoreMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.readscore.ReadAuthorScoreReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 根据用户对图书打分计算用户对作者打分
 * 输入：用户图书打分结果表 dmn.irecm_us_bkid_score_all
 * msisdn|book_id|book_score
 * 过滤打分过低的图书
 * 输出:key:msisdn|authorid; value:score
 *
 * @throws java.io.IOException
 * @throws InterruptedException
 * @throws ClassNotFoundException
 */
public class ReadAuthorScoreJobDriver extends Configured implements Tool {
    private PluginUtil pluginUtil = null;
    private LogUtil logUtil = null;
    private int reduceNo = 5;
    private int reduceNoBig = 20;
    private JobExecuUtil execuUtil = null;

    public ReadAuthorScoreJobDriver(String conf, String dateStr) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.load(conf, dateStr);
        logUtil = LogUtil.getInstance();
        reduceNo = pluginUtil.getReduceNum();
        reduceNoBig = pluginUtil.getReduceNumBig();
        execuUtil = new JobExecuUtil();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: AuthorRecommDriver conf [date]");
            return;
        }
        String conf = args[0];
        Date begin = new Date();
        String dateStr = null;
        if (args.length > 1) {
            dateStr = args[1];
            if (dateStr.length() != 8) {
                System.err.println("date error. eg: 20150610");
                return;
            }
        }
        ReadAuthorScoreJobDriver driver = new ReadAuthorScoreJobDriver(conf, dateStr);
        int ret = ToolRunner.run(new Configuration(), driver, args);
        Date end = new Date();
        long timeCost = end.getTime() - begin.getTime();
        String timeInfo = String.format("time cost in total(s): %d", timeCost / 1000);
        driver.logUtil.getLogger().info(timeInfo);
        System.exit(ret);
    }

    @Override
    public int run(String[] strings) throws Exception {
        String jobLogStr = "calcu read author score";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(jobLogStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setFloat("bookScoreMin", (float) pluginUtil.getBookScoreMin());
        conf.setBoolean("bUseNewHadoop", pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> passKeyValues = new HashMap<String, PluginUtil.WorkPath>();
        //图书-作者
        String key = PluginUtil.BOOKINFO_KEY;
        passKeyValues.put(key, pluginUtil.getPath(key));

        conf.set(key, pluginUtil.getPath(key).getPathValue());
        logUtil.getLogger().info("书-作者: " + pluginUtil.getPath(key).getPathValue());


        //作者-分类-上架图书数量。bookinfoDataPrepareJob的结果
        key = PluginUtil.TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY;
        passKeyValues.put(key, pluginUtil.getPath(key));

        conf.set(key, pluginUtil.getPath(key).getPathValue());
        logUtil.getLogger().info("作者分类上架图书: " + pluginUtil.getPath(key).getPathValue());

        logUtil.getLogger().info("======================================");
        System.out.println(passKeyValues);
        logUtil.getLogger().info(passKeyValues);
        logUtil.getLogger().info("======================================");

        Job job = execuUtil.newJobAndAddCacheFile(conf, passKeyValues);
        job.setJobName("readAuthorScore");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ReadAuthorScoreMapper.class);
        job.setReducerClass(ReadAuthorScoreReducer.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        String scorePath = pluginUtil.getPath(PluginUtil.USER_BOOK_SCORE_KEY).getPathValue();
        String scoreOut = pluginUtil.getPath(PluginUtil.READ_AUTHOR_SCORE_OUT_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(scorePath));
        FileOutputFormat.setOutputPath(job, new Path(scoreOut));

        execuUtil.checkOutputPath(scoreOut);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, jobLogStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, jobLogStr, false));
            return 1;
        }
    }
}

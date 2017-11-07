package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.display.*;
import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitioner;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommDisplayDriver extends WrapDriver {

    private PluginUtil.WorkPath blacklistCheckPath = null;
    private String blacklistOutKey = PluginUtil.RECOMM_BLACKLIST_OUT_KEY;
    private String recommDisplayResultOut = null;
    private String recommResultOut = null;
    private int currTurn = -1;
    private String prefSplits = null;

    public RecommDisplayDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
        blacklistCheckPath = pluginUtil.getPath(blacklistOutKey);
        recommDisplayResultOut = pluginUtil.getPath(
                PluginUtil.RECOMM_DISPLAY_RESULT_OUT_KEY).getPathValue();
        recommResultOut = execuUtil.getMROutSliptFiles(pluginUtil.getPath(
                PluginUtil.RECOMM_RESULT_OUT_KEY).getPathValue());
    }

    public RecommDisplayDriver(Configuration cf, int turn) {
        super(cf);
        // TODO Auto-generated constructor stub
        blacklistCheckPath = pluginUtil.getPath(blacklistOutKey);
        this.currTurn = turn;
        recommDisplayResultOut = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.RECOMM_DISPLAY_RESULT_OUT_KEY)
                        .getPathValue());
        recommResultOut = execuUtil.getMROutSliptFiles(execuUtil
                .getWriteOutputForTurns(turn, pluginUtil.getPath(
                        PluginUtil.RECOMM_RESULT_OUT_KEY).getPathValue()));
    }

    public void setPrefSplitPath(String prefPath) {
        this.prefSplits = prefPath;
    }

    /**
     * 从推荐结果表中筛选数据推荐给用户
     * 输入：merge已读、预测、补白后的推荐结果表
     * 格式：msisdn|authorid|bookid|score|type
     * 规则：剔除黑名单图书，根据筛选、轮换规则
     * 输出：msisdn|authorid|bookid|type|展示位置
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public int dispalyRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "display recomm job";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        conf.setInt(PluginUtil.RECOMM_AUTHOR_NUM, pluginUtil.getTotalRecommNum());
        conf.setInt(PluginUtil.RECOMM_AUTHOR_NUM_READ, pluginUtil.getReadRecommNum());
        conf.setInt(PluginUtil.RECOMM_AUTHOR_NUM_TYPE4, pluginUtil.getType4RecommNum());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(blacklistOutKey, blacklistCheckPath);
        String key = PluginUtil.TEMP_TORECOMM_USER_PREF_FILTER_KEY;
        String prefPath = execuUtil.getPartsMROutSliptFiles(
                pluginUtil.getPath(key).getPathValue());
        PluginUtil.WorkPath path = null;
        if (currTurn >= 0) {
            path = pluginUtil.new WorkPath(prefSplits, true, true);
        } else {
            path = pluginUtil.new WorkPath(prefPath, false, true);
        }
        paraMap.put(key, path);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobname = String.format("dispalyRecomm%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(DisplayMapper.class);
        job.setReducerClass(DisplayReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecommItemWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(recommResultOut));
        FileOutputFormat.setOutputPath(job, new Path(recommDisplayResultOut));
        execuUtil.checkOutputPath(recommDisplayResultOut);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(
                    execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(
                    execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }

    /**
     * 读取黑名单
     * 输入：历史黑名单（近5天）、前一天的推荐展示结果
     * 格式：msisdn|bookid
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public int blacklistJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "get recomm blacklist job";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.set("DATESTR", pluginUtil.getDateStr());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("blacklist");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String lastdayRecommRolling = pluginUtil.getPath(
                PluginUtil.RECOMM_RESULT_OUT_KEY).getPathValueLastday();
        String blacklistPath = pluginUtil.getPath(
                PluginUtil.RECOMM_BLACKLIST_OUT_KEY).getPathValueLastday();
        FileSystem fs = FileSystem.get(URI.create(lastdayRecommRolling), conf);
        Path f = new Path(lastdayRecommRolling);
        boolean isExists = fs.exists(f);
        boolean bHasBlacklist = false;
        if (isExists) {
            bHasBlacklist = true;
            MultipleInputs.addInputPath(job, new Path(lastdayRecommRolling),
                    TextInputFormat.class, BlacklistLastdayRecommMapper.class);
        }
        fs = FileSystem.get(URI.create(blacklistPath), conf);
        f = new Path(blacklistPath);
        isExists = fs.exists(f);
        if (isExists) {
            bHasBlacklist = true;
            MultipleInputs.addInputPath(job, new Path(blacklistPath),
                    TextInputFormat.class, BlacklistFilterMapper.class);
        }
        if (!bHasBlacklist) {
            return 0;
        }
        String blacklistOut = blacklistCheckPath.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(blacklistOut));
        execuUtil.checkOutputPath(blacklistOut);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(
                    execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(
                    execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }

    public int chechResultJob() throws IOException,
            InterruptedException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        String logStr = "check recomm result";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.BOOK_DETAIL_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("chechResult");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(CheckRecommResultMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(recommDisplayResultOut));
        String checkResultPath = pluginUtil.getPath(PluginUtil
                .TEMP_DEBUG_CHECK_RESULT_OUT).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(checkResultPath));
        execuUtil.checkOutputPath(checkResultPath);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(
                    execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(
                    execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }
}

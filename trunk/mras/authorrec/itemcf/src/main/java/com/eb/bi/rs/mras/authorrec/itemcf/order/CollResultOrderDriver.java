package com.eb.bi.rs.mras.authorrec.itemcf.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.WrapDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CollResultOrderDriver extends WrapDriver {

    private String prefSplitKey = PluginUtil.TEMP_USER_PREF_CLASS_SPLIT_OUT;
    private String prefSplitPath = null;
    private String predictPartPath = null;
    private String prefAuthorFilterOut = null;
    private String recommOutPath = null;
    private int currTurn = -1;

    public CollResultOrderDriver(Configuration cf, int turn, String prefPath,
                                 String predictPath) {
        super(cf);
        currTurn = turn;
        prefSplitPath = prefPath;
        predictPartPath = predictPath;
        prefAuthorFilterOut = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_PREDICT_FILTER_AUTHOR_PREF_KEY)
                        .getPathValue());
        recommOutPath = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY)
                        .getPathValue());
    }

    public int orderByPref()
            throws IOException, ClassNotFoundException, InterruptedException {
        int prefStatu = prefAuthorPredictJob();
        if (prefStatu != 0) {
            return prefStatu;
        }
        int orderStatu = orderRecommJob();
        if (orderStatu != 0) {
            return orderStatu;
        }
        return 0;
    }

    /**
     * 关联用户偏好表与用户作者预测打分表
     * 输入：筛选后的用户-作者-预测打分表
     * 格式：msisdn|predictauthorid|predictscore
     * 关联：关联用户偏好表、作者分类表
     * 输出: msisdn|偏好n|偏好classid|authorid1|score1|authorid2|score2...
     * value按score从大到小排序
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int prefAuthorPredictJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "correlate user prefer and predict score";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        PluginUtil.WorkPath path = pluginUtil.new WorkPath(
                pluginUtil.getPath(prefSplitKey).getPathValue(), false, true);
        path.setIsVirtual(true);
        paraMap.put(prefSplitKey, path);
        List<String> prefPaths = new ArrayList<String>();
        prefPaths.add(prefSplitPath);
        String key = PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, prefPaths);
        String jobname = String.format("prefAuthorPredict%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PrefAuthorFilterMapper.class);
        job.setReducerClass(PrefAuthorFilterReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(this.predictPartPath)));
        FileOutputFormat.setOutputPath(job, new Path(prefAuthorFilterOut));
        execuUtil.checkOutputPath(prefAuthorFilterOut);
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
     * 预测打分作者推荐：根据用户-作者预测打分表，关联用户偏好，按照作家打分高低选排名入库。
     * 输入：筛选后的用户作者打分表，prefAuthorPredictJob的结果
     * msisdn|偏好n|偏好classid|authorid1|score1|authorid2|score2...
     * 输出: msisdn|authorid|bookid|score|type
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int orderRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "predict recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.BOOKINFO_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        key = PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobname = String.format("recommendPredictAuthor%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PredictRecommMapper.class);
        job.setReducerClass(PredictRecommReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RecommItemWritable.class);
        FileInputFormat.setInputPaths(job, new Path(
                execuUtil.getMROutSliptFiles(prefAuthorFilterOut)));
        FileOutputFormat.setOutputPath(job, new Path(recommOutPath));
        execuUtil.checkOutputPath(recommOutPath);
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

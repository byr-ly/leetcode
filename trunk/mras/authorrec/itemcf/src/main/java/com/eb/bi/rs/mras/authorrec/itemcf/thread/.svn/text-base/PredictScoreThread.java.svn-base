package com.eb.bi.rs.mras.authorrec.itemcf.thread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.PredictWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.collaborate.PredictOnceCombiner;
import com.eb.bi.rs.mras.authorrec.itemcf.collaborate.PredictOnceMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.collaborate.PredictOnceReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PredictRecommMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PredictRecommReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PrefAuthorFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PrefAuthorFilterReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PredictScoreThread implements Runnable {

    private int turn = 0;
    private String userScorePath = null;
    private Configuration configuration = null;
    private PluginUtil pluginUtil = PluginUtil.getInstance();
    private LogUtil logUtil = LogUtil.getInstance();
    private JobExecuUtil execuUtil = new JobExecuUtil();
    private String authorReindxKey = PluginUtil.TEMP_COLL_AUTHOR_REINDEX_KEY;
    private PluginUtil.WorkPath authorRedinxPath = null;
    private String scoreAfterFilterPath = null;
    private String authorSimKey = PluginUtil.TEMP_AUTHOR_SIMILARITY_KEY;
    private PluginUtil.WorkPath authorSimWorkPath = null;
    private String authorUserPredictOut = null;
    private String prefSplitKey = PluginUtil.TEMP_USER_PREF_CLASS_SPLIT_OUT;
    private String prefSplitPath = null;
    private String prefAuthorFilterOut = null;
    private String recommOutPath = null;
    private boolean bSuccess = false;
    private String statisticStr;

    public PredictScoreThread(int id, String scorePath, String usPath,
                              String prefPath, Configuration c) {
        this.turn = id;
        this.userScorePath = usPath;
        this.prefSplitPath = prefPath;
        this.configuration = c;
        authorRedinxPath = pluginUtil.getPath(authorReindxKey);
        authorSimWorkPath = pluginUtil.getPath(authorSimKey);
        scoreAfterFilterPath = scorePath;
        prefAuthorFilterOut = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_PREDICT_FILTER_AUTHOR_PREF_KEY)
                        .getPathValue());
        recommOutPath = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY)
                        .getPathValue());
    }

    public boolean isSuccess() {
        return this.bSuccess;
    }

    public String getStatisticStr() {
        return this.statisticStr;
    }

    public int getThreadId() {
        return this.turn;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        try {
            int predictStatu = predictScoreJobOnce();
            if (predictStatu != 0) {
                return;
            }
            int orderStatu = orderByPref();
            if (orderStatu != 0) {
                return;
            }
            execuUtil.deleteOutputPath(authorUserPredictOut);
            bSuccess = true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 拆分用户
     */
    private int predictScoreJobOnce()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = String.format("calcu predict score once %d", turn);
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(this.configuration);
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        conf.setFloat(PluginUtil.COLL_AUTHOR_SIM_MIN,
                (float) pluginUtil.getAuthorSimMin());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(authorReindxKey, authorRedinxPath);
        String key = PluginUtil.TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT;
        PluginUtil.WorkPath path = pluginUtil.new WorkPath(scoreAfterFilterPath, false, true);
        path.setIsVirtual(true);
        paraMap.put(key, path);
        List<String> userPathes = new ArrayList<String>();
        userPathes.add(userScorePath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, userPathes);
        String jobname = "predictScoreOnce";
        if (pluginUtil.isDebug()) {
            jobname = String.format("%sd%d", jobname, turn);
        } else {
            jobname = String.format("%s%d", jobname, turn);
        }
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PredictOnceMapper.class);
        job.setCombinerClass(PredictOnceCombiner.class);
        job.setReducerClass(PredictOnceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PredictWritable.class);
        job.setNumReduceTasks(pluginUtil.getReduceNumBig());
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(authorSimWorkPath.getPathValue())));
        authorUserPredictOut = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_PREDICT_SCORE_PARTS_KEY)
                        .getPathValue());
        FileOutputFormat.setOutputPath(job, new Path(authorUserPredictOut));
        execuUtil.checkOutputPath(authorUserPredictOut);
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
        Configuration conf = new Configuration(this.configuration);
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
        String jobname = String.format("prefAuthorPredict%d", turn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PrefAuthorFilterMapper.class);
        job.setReducerClass(PrefAuthorFilterReducer.class);
        job.setNumReduceTasks(pluginUtil.getReduceNum());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(this.authorUserPredictOut)));
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
        Configuration conf = new Configuration(this.configuration);
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.BOOKINFO_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        key = PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobname = String.format("recommendPredictAuthor%d", turn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PredictRecommMapper.class);
        job.setReducerClass(PredictRecommReducer.class);
        job.setNumReduceTasks(pluginUtil.getReduceNum());
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

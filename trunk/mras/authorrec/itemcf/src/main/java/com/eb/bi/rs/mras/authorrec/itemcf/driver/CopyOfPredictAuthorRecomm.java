package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.filler.ToRecommFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PredictRecommMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PredictRecommReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PrefAuthorFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.order.PrefAuthorFilterReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CopyOfPredictAuthorRecomm extends CommRecommDriver {

    private String predictUserAuthorScorePath = null;
    private String torecommUserAuthorOut = null;
    private String prefAuthorFilterOut = null;

    public CopyOfPredictAuthorRecomm(Configuration cf, String initpath) {
        super(cf, initpath);
        predictUserAuthorScorePath = execuUtil.getPartsMROutSliptFiles(pluginUtil.getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue());
        torecommUserAuthorOut = pluginUtil.getPath(PluginUtil.TEMP_TORECOMM_USER_PREIDICT_KEY).getPathValue();
        prefAuthorFilterOut = pluginUtil.getPath(PluginUtil.TEMP_PREDICT_FILTER_AUTHOR_PREF_KEY).getPathValue();
        recommOutPath = pluginUtil.getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue();
        userPrefFilterPath = pluginUtil.getPath(userPrefFilterKey);
    }

    public CopyOfPredictAuthorRecomm(Configuration cf, String initPath,
                                     int turn, List<String> initParts) {
        super(cf, initPath, turn, initParts);
        predictUserAuthorScorePath = execuUtil.getPartsMROutSliptFiles(pluginUtil.
                getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue());

        torecommUserAuthorOut = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.
                getPath(PluginUtil.TEMP_TORECOMM_USER_PREIDICT_KEY).getPathValue());

        prefAuthorFilterOut = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.
                getPath(PluginUtil.TEMP_PREDICT_FILTER_AUTHOR_PREF_KEY).getPathValue());

        recommOutPath = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.
                getPath(PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue());

        String path = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.
                getPath(userPrefFilterKey).getPathValue());
        userPrefFilterPath = pluginUtil.new WorkPath(path, true, true);
    }

    /**
     * 对预测打分表和偏好表筛选待推荐用户
     * 输入：用户-作者预测打分表
     * 格式：msisdn|authorid|score
     * 过滤：用户不在指定待推荐用户库中的数据
     * 指定待推荐用户库：若是初始化，使用智能推荐基础数据近6月累计表中的数据；否则使用智能推荐待推荐用户列表中的数据
     * msisdn
     * 输出: authorid|authorid|score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int toRecommUserFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "filter user for predict recommend";
        long start = System.currentTimeMillis();
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        PluginUtil.WorkPath workPath = pluginUtil.new WorkPath(this.initUserPath, false, true);
        if (!pluginUtil.isIncrementUpdate() && currTurn >= 0) {
            workPath.setIsVirtual(true);
        }
        paraMap.put("initUserPath", workPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, partFilePathes);
        job.setJobName(String.format("toRecommUserFilterPredict%d", currTurn));
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ToRecommFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(this.predictUserAuthorScorePath));
        FileOutputFormat.setOutputPath(job, new Path(this.torecommUserAuthorOut));
        execuUtil.checkOutputPath(this.torecommUserAuthorOut);
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
        paraMap.put(userPrefFilterKey, userPrefFilterPath);
        String key = PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("prefAuthorPredict");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PrefAuthorFilterMapper.class);
        job.setReducerClass(PrefAuthorFilterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(this.torecommUserAuthorOut)));
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
    @Override
    protected int orderRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        int prefAuthorStatu = prefAuthorPredictJob();
        if (prefAuthorStatu != 0) {
            return prefAuthorStatu;
        }

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
        job.setJobName("recommendPredictAuthor");
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

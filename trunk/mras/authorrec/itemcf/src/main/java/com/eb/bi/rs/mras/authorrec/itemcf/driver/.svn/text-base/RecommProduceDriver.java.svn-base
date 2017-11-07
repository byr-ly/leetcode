package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.filler.NoFillerUserFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.filler.NoFillerUserFilterReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitioner;
import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitionerI;
import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitionerN;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.*;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RecommProduceDriver extends WrapDriver {

    private String initUserPath = null;
    private int currTurn = -1;
    //output

    private String recommResultOutPath = null;
    private String noFillerUserKey = PluginUtil.TEMP_NO_FILLER_USER_KEY;
    private PluginUtil.WorkPath noFillerUserPath = null;

    private ReadAuthorRecomm readAuthorRecomm = null;
    private PredictAuthorRecomm predictAuthorRecomm = null;
    private FillerAuthorRecomm fillerAuthorRecomm = null;
    private RecommDisplayDriver displayDriver = null;
    private int userSplitNum = -1;


    public RecommProduceDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
        if (pluginUtil.isIncrementUpdate()) {
            initUserPath = pluginUtil.getPath(
                    PluginUtil.TORECOMM_USER_INC_KEY).getPathValue();
        } else {
            initUserPath = pluginUtil.getPath(
                    PluginUtil.USER_READ_DEPTH_6CM_KEY).getPathValue();
        }
        recommResultOutPath = pluginUtil.getPath(
                PluginUtil.RECOMM_RESULT_OUT_KEY).getPathValue();
        noFillerUserPath = pluginUtil.getPath(noFillerUserKey);
        userSplitNum = pluginUtil.getRecommUserSplitNum();
        if (userSplitNum <= 0) {
            userSplitNum = reduceNoBig;
        }
    }

    public int productRecommJob()
            throws ClassNotFoundException, IOException, InterruptedException {
        int reorderUserStatu = reorderToremcommUserJob();
        if (reorderUserStatu != 0) {
            return reorderUserStatu;
        }
        int reorderDepthStatu = reorderUserReadDepthJob();
        if (reorderDepthStatu != 0) {
            return reorderDepthStatu;
        }
        int reorderNoFillerStatu = reorderNoFillerUserJob();
        if (reorderNoFillerStatu != 0) {
            return reorderNoFillerStatu;
        }
        int recommStatu = recommJob();
        if (recommStatu != 0) {
            return recommStatu;
        }
        return 0;
    }


    /**
     * 整理待推荐用户
     */
    private int reorderToremcommUserJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "reorder torecomm users";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("reorderToremcommUsers");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(ReorderTorecommUserMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setPartitionerClass(UserPartitionerN.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(userSplitNum);
        FileInputFormat.setInputPaths(job, new Path(initUserPath));
        String outPath = pluginUtil.getPath(PluginUtil
                .TEMP_REORDER_INIT_USER_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        execuUtil.checkOutputPath(outPath);
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
     * 整理阅读数据
     * 过滤未阅读记录
     */
    private int reorderUserReadDepthJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "reorder user read depth";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.TEMP_REORDER_INIT_USER_KEY;
        PluginUtil.WorkPath path = pluginUtil.getPath(key);
        if (!pluginUtil.isIncrementUpdate()) {
            path.setIsVirtual(true);
        }
        paraMap.put(key, path);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("reorderUserReadDepth");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(ReorderUserReadDepthMapper.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setCombinerClass(DistinctItemReducer.class);
        job.setReducerClass(DistinctItemReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(userSplitNum);
        String readPath = pluginUtil.getPath(PluginUtil
                .USER_BOOK_READ_DEPTH_HISTORY_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(readPath));
        String outPath = pluginUtil.getPath(PluginUtil
                .TEMP_REORDER_READ_DEPTH_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        execuUtil.checkOutputPath(outPath);
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
     * 只保留不需要补白的用户，保留预测的图书数目
     * 输入：预测用户推荐结果
     * 格式：msisdn|authorid|bookid|score|type
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int reorderNoFillerUserJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "no Filler User Filter";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setInt(PluginUtil.RECOMM_AUTHOR_NUM, pluginUtil.getTotalRecommNum());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("noFillerUserFilter");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(NoFillerUserFilterMapper.class);
        job.setReducerClass(NoFillerUserFilterReducer.class);
        job.setPartitionerClass(UserPartitionerI.class);
        job.setNumReduceTasks(userSplitNum);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        String key = PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY;
        String path = execuUtil.getPartsMROutSliptFiles(
                pluginUtil.getPath(key).getPathValue());
        FileInputFormat.setInputPaths(job, new Path(path));
        String outPath = noFillerUserPath.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        execuUtil.checkOutputPath(outPath);
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

    private int recommJob()
            throws ClassNotFoundException, IOException, InterruptedException {
        int splitno = pluginUtil.getRecommUserSplitNum();
        if (splitno <= 0) {
            return recommJobOnce();
        }
        initUserPath = pluginUtil.getPath(PluginUtil
                .TEMP_REORDER_INIT_USER_KEY).getPathValue();
        String readDepthPath = pluginUtil.getPath(PluginUtil
                .TEMP_REORDER_READ_DEPTH_KEY).getPathValue();
        int turnPart = pluginUtil.getInitUserPart();
        int turnSizes = 0;
        if (turnPart <= 0) {
            return recommJobOnce();
        }
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] status = fs.globStatus(new Path(
                execuUtil.getMROutSliptFiles(initUserPath)));
        FileStatus[] readDepthStatus = fs.globStatus(new Path(
                execuUtil.getMROutSliptFiles(readDepthPath)));
        FileStatus[] noFillerStatus = fs.globStatus(new Path(execuUtil
                .getMROutSliptFiles(noFillerUserPath.getPathValue())));
        List<String> initUsers = new ArrayList<String>();
        List<String> readDepths = new ArrayList<String>();
        List<String> noFillerUsers = new ArrayList<String>();
        for (FileStatus st : status) {
            initUsers.add(st.getPath().toString());
        }
        Collections.sort(initUsers);
        for (FileStatus st2 : readDepthStatus) {
            readDepths.add(st2.getPath().toString());
        }
        Collections.sort(readDepths);
        for (FileStatus st3 : noFillerStatus) {
            noFillerUsers.add(st3.getPath().toString());
        }
        Collections.sort(noFillerUsers);
        turnSizes = initUsers.size() / turnPart;
        if (turnSizes * turnPart < initUsers.size()) {
            turnSizes++;
        }
//		boolean bSameSize = (initUsers.size() == readDepths.size());
        for (int i = 0; i < turnSizes; i++) {
            List<String> pathes = new ArrayList<String>();
            List<String> readDepthPathes = new ArrayList<String>();
            List<String> noFillerPathes = new ArrayList<String>();
            int start = i * turnPart;
            for (int j = start; (j < start + turnPart)
                    && (j < initUsers.size()); j++) {
                pathes.add(initUsers.get(j));
                readDepthPathes.add(readDepths.get(j));
                noFillerPathes.add(noFillerUsers.get(j));
            }
            currTurn = i;
            int recommstatus = recommJobOnce(i, pathes, readDepthPathes, noFillerPathes);
            if (recommstatus != 0) {
                return recommstatus;
            }
        }
        return 0;
    }

    private int recommJobOnce(int turn, List<String> pathes,
                              List<String> readpathes, List<String> noFillerPathes)
            throws ClassNotFoundException, IOException, InterruptedException {
        readAuthorRecomm = new ReadAuthorRecomm(initConf, initUserPath, turn, pathes);
        readAuthorRecomm.setReadDepthParts(readpathes);
        predictAuthorRecomm = new PredictAuthorRecomm(initConf, initUserPath, turn, pathes);
        fillerAuthorRecomm = new FillerAuthorRecomm(initConf, initUserPath, turn, pathes);
        fillerAuthorRecomm.setReadDepthParts(readpathes);
        fillerAuthorRecomm.setNoFillerUserPath(noFillerPathes);
        displayDriver = new RecommDisplayDriver(initConf, turn);
        return recommJobOnce();
    }

    private int recommJobOnce()
            throws ClassNotFoundException, IOException, InterruptedException {
        if (readAuthorRecomm == null) {
            readAuthorRecomm = new ReadAuthorRecomm(initConf, initUserPath);
        }
        if (predictAuthorRecomm == null) {
            predictAuthorRecomm = new PredictAuthorRecomm(initConf, initUserPath);
        }
        if (fillerAuthorRecomm == null) {
            fillerAuthorRecomm = new FillerAuthorRecomm(initConf, initUserPath);
        }
        if (displayDriver == null) {
            displayDriver = new RecommDisplayDriver(initConf);
        }
        int readRecomm = readAuthorRecomm.recommendJob();
        if (readRecomm != 0) {
            return readRecomm;
        }
        int predictRecomm = predictAuthorRecomm.recommendJob();
        if (predictRecomm != 0) {
            return predictRecomm;
        }
        int fillerRecomm = fillerAuthorRecomm.recommendJob();
        if (fillerRecomm != 0) {
            return fillerRecomm;
        }
        int resultMergeStatu = mergeRecommResultJob();
        if (resultMergeStatu != 0) {
            return resultMergeStatu;
        }
        String prefPath = fillerAuthorRecomm.getPrefSplitPath();
        displayDriver.setPrefSplitPath(prefPath);
        int displayStatu = displayDriver.dispalyRecommJob();
        if (displayStatu != 0) {
            return displayStatu;
        }
        return 0;
    }

    /**
     * 从推荐结果表中筛选数据推荐给用户
     * 输入：已读推荐结果表、预测推荐结果表、补白推荐结果表
     * 格式：msisdn|authorid|bookid|score|type
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int mergeRecommResultJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "merge recomm result job";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("mergeRecommResult");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(RecommResultMergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputValueClass(RecommItemWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String readRecommOut = null;
        String predictRecommOut = null;
        String fillerRecommOut = null;
        if (currTurn >= 0) {
            readRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_READ_RESULT_KEY).getPathValue();
            readRecommOut = execuUtil.getMROutSliptFiles(
                    execuUtil.getWriteOutputForTurns(currTurn, readRecommOut));
            predictRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_PREDICT_RESULT_ORDER_KEY).getPathValue();
            predictRecommOut = execuUtil.getMROutSliptFiles(
                    execuUtil.getWriteOutputForTurns(currTurn, predictRecommOut));
            fillerRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_FILLER_RESULT_KEY).getPathValue();
            fillerRecommOut = execuUtil.getMROutSliptFiles(
                    execuUtil.getWriteOutputForTurns(currTurn, fillerRecommOut));
        } else {
            readRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_READ_RESULT_KEY).getPathValue();
            predictRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue();
            predictRecommOut = execuUtil.getPartsMROutSliptFiles(predictRecommOut);
            fillerRecommOut = pluginUtil.getPath(
                    PluginUtil.TEMP_RECOMM_FILLER_RESULT_KEY).getPathValue();
        }
        FileInputFormat.addInputPath(job, new Path(readRecommOut));
        FileInputFormat.addInputPath(job, new Path(predictRecommOut));
        FileInputFormat.addInputPath(job, new Path(fillerRecommOut));
        String resultOutPath = recommResultOutPath;
        if (currTurn >= 0) {
            resultOutPath = execuUtil.getWriteOutputForTurns(currTurn, resultOutPath);
        }
        FileOutputFormat.setOutputPath(job, new Path(resultOutPath));
        execuUtil.checkOutputPath(resultOutPath);
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

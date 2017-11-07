package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.PredictWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.ScoreWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.SimilarityWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.author.ReorderAuthorDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.collaborate.*;
import com.eb.bi.rs.mras.authorrec.itemcf.order.CollResultOrderDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.user.CollUserFilterDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CoordinateFilterDriver extends WrapDriver {

    class JobOutPathClass {
        int index;
        Job job;
        String outPath;

        public JobOutPathClass(int id, Job j, String path) {
            this.index = id;
            this.job = j;
            this.outPath = path;
        }
    }

    private String authorSimKey = PluginUtil.TEMP_AUTHOR_SIMILARITY_KEY;
    private PluginUtil.WorkPath authorSimWorkPath = null;
    private String authorReindxKey = PluginUtil.TEMP_COLL_AUTHOR_REINDEX_KEY;
    private PluginUtil.WorkPath authorRedinxPath = null;
    private String scoreAfterFilterPath = null;
    private String prefPath = null;
//	private String authorUserPredictOut = null;

    public CoordinateFilterDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
//		authorAvgWorkPath = pluginUtil.getPath(authorAvgKey);
        authorSimWorkPath = pluginUtil.getPath(authorSimKey);
        authorRedinxPath = pluginUtil.getPath(authorReindxKey);

    }

    /**
     * 协调过滤计算
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * 推荐库表——input
     * bookinfo、author表
     * 输出: 预测打分。msisdn|authorid|score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public int coordinateFilteringJob()
            throws ClassNotFoundException, IOException, InterruptedException {
        CollUserFilterDriver userFilter = new CollUserFilterDriver(getConf());
        int userFilterStatu = userFilter.filterJob();
        if (userFilterStatu != 0) {
            return userFilterStatu;
        }

        prefPath = execuUtil.getMROutSliptFiles(userFilter.getPrefSplitPath());
        ReorderAuthorDriver authorDriver = new ReorderAuthorDriver(getConf());
        int authorStatu = authorDriver.reorder();
        if (authorStatu != 0) {
            return authorStatu;
        }
        scoreAfterFilterPath =
                execuUtil.getMROutSliptFiles(authorDriver.getReindxUserAuthorScorePath());

        int authorSimStatu = authorSimilarityJob();
        if (authorSimStatu != 0) {
            return authorSimStatu;
        }
        String scoreMergeOut = pluginUtil.getPath(PluginUtil.TEMP_USER_AUTHOR_SCORE_MERGE_KEY).getPathValue();
        execuUtil.deleteOutputPath(scoreMergeOut);

        String predictUserPath = userFilter.predictUserPrepare(scoreAfterFilterPath);
        int predictStatu = predictScorePartsJob(execuUtil.getMROutSliptFiles(predictUserPath));
        if (predictStatu != 0) {
            return predictStatu;
        }
        return 0;
    }


    /**
     * 协调过滤数据准备：计算作者相似性。
     * Job1：对同一用户打分的多个作者两两组合
     * 输入：筛选后的用户作者打分表，predictAuthorFilterPrepareJob的结果
     * msisdn|authorid|score
     * 输出: authoridI|authoridJ|scoreI|scoreJ
     * Job2：计算作者相似性
     * 输入：Job1输出
     * 输出：authoridI|authoridJ|sim
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int authorSimilarityJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "merge authors of same user for similarity calcuing";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("predictAuthorMergePrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(UserAuthorsMergerMapper.class);
        job.setReducerClass(UserAuthorsMergerReducer.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);

        String scoreMergeOut = pluginUtil.getPath(PluginUtil.TEMP_USER_AUTHOR_SCORE_MERGE_KEY).getPathValue();
        execuUtil.checkOutputPath(scoreMergeOut);

        FileInputFormat.setInputPaths(job, new Path(scoreAfterFilterPath));
        FileOutputFormat.setOutputPath(job, new Path(scoreMergeOut));


        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));

            return 1;
        }

        int similar = similarJob();
        return similar;
    }

    private int similarJob() throws IOException,
            InterruptedException, ClassNotFoundException {
        //计算相似性
        long start = System.currentTimeMillis();
        String logStr = "calcu author similarity";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setInt(PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY,
                pluginUtil.getAuthorCommUserMin());
        conf.setFloat(PluginUtil.COLL_AUTHOR_SIM_MIN,
                pluginUtil.getAuthorSimMin());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("authorSimilarity");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(AuthorSimilarityMapper.class);
        job.setReducerClass(AuthorSimilarityReducer.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SimilarityWritable.class);
        String scoreMergeOut = pluginUtil.getPath(
                PluginUtil.TEMP_USER_AUTHOR_SCORE_MERGE_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(
                execuUtil.getMROutSliptFiles(scoreMergeOut)));
        String authorSimilarityOut = authorSimWorkPath.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(authorSimilarityOut));
        execuUtil.checkOutputPath(authorSimilarityOut);
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


    public int predictScorePartsJob(String userScorePath)
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "calcu predict score parts";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] status = fs.globStatus(new Path(userScorePath));
        List<String> splitPathes = new ArrayList<String>();
        FileStatus[] prefStatus = fs.globStatus(new Path(prefPath));
        List<String> prefPathes = new ArrayList<String>();
        if (status.length != prefStatus.length) {
            String err = String.format("predictScorePartsJob split size not equal");
            logUtil.getLogger().error(err);
            return 1;
        }
        for (int i = 0; i < status.length; i++) {
            String path = status[i].getPath().toString();
            splitPathes.add(path);
        }
        for (int i = 0; i < prefStatus.length; i++) {
            String path = prefStatus[i].getPath().toString();
            prefPathes.add(path);
        }
        Collections.sort(splitPathes);
        Collections.sort(prefPathes);
        //串行
        /*for (int i = 0; i < splitPathes.size(); i ++)
        {
			int predictStatu = predictScoreJobOnce(i, splitPathes.get(i));
			if (predictStatu != 0)
			{
				continue;
			}
			CollResultOrderDriver orderDriver = new CollResultOrderDriver(
					getConf(), i, prefPathes.get(i), authorUserPredictOut);
			int orderStatu = orderDriver.orderByPref();
			if (orderStatu != 0)
			{
				continue;
			}
			execuUtil.deleteOutputPath(authorUserPredictOut);
		}*/

        //并行
        int concurrency = pluginUtil.getCollParalleNum();
        List<JobOutPathClass> paralleJobs
                = new ArrayList<JobOutPathClass>();
        boolean bLast = false;
        for (int i = 0; i < splitPathes.size(); i++) {
            if (i == splitPathes.size() - 1) {
                bLast = true;
            }
            if (paralleJobs.size() < concurrency) {
                JobOutPathClass predictJob
                        = predictScoreJobOnce(i, splitPathes.get(i));
                paralleJobs.add(predictJob);
                if (paralleJobs.size() < concurrency && !bLast) {
                    continue;
                }
            }
            int initSize = paralleJobs.size();
            while (paralleJobs.size() == initSize) {
                for (int j = 0; j < paralleJobs.size(); j++) {
                    JobOutPathClass entry = paralleJobs.get(j);
                    int turn = entry.index;
                    if (!entry.job.isComplete()) {
                        continue;
                    }
                    entry = paralleJobs.remove(j);
                    j--;
                    if (!entry.job.isSuccessful()) {
                        continue;
                    }
                    String outPath = entry.outPath;
                    CollResultOrderDriver orderDriver = new CollResultOrderDriver(
                            getConf(), turn, prefPathes.get(turn), outPath);
                    int orderStatu = orderDriver.orderByPref();
                    if (orderStatu != 0) {
                        continue;
                    }
                    execuUtil.deleteOutputPath(outPath);
                }
            }
        }
        return 0;
    }

    /**
     * 拆分用户，并行
     */
    private JobOutPathClass predictScoreJobOnce(int turn, String userPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = String.format("calcu predict score once %d", turn);
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
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
        userPathes.add(userPath);
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
        job.setNumReduceTasks(reduceNoBig);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(authorSimWorkPath.getPathValue())));
        String predictOut = execuUtil.getWriteOutputForTurns(turn,
                pluginUtil.getPath(PluginUtil.TEMP_PREDICT_SCORE_PARTS_KEY)
                        .getPathValue());
        FileOutputFormat.setOutputPath(job, new Path(predictOut));
        execuUtil.checkOutputPath(predictOut);
        job.submit();
        JobOutPathClass jobOut = new JobOutPathClass(turn, job, predictOut);
        return jobOut;
    }

    /**
     * 拆分用户，串行
     */
/*	private int predictScoreJobOnce(int turn, String userPath) 
            throws IOException, ClassNotFoundException, InterruptedException
	{
		long start = System.currentTimeMillis();
		String logStr = String.format("calcu predict score once %d", turn);
		logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
		conf.setFloat(PluginUtil.COLL_AUTHOR_SIM_MIN, 
				(float)pluginUtil.getAuthorSimMin());
		Map<String, WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
		paraMap.put(authorReindxKey, authorRedinxPath);
		String key = PluginUtil.TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT;
		WorkPath path = pluginUtil.new WorkPath(scoreAfterFilterPath, false, true);
		path.setIsVirtual(true);
		paraMap.put(key, path);
		List<String> userPathes = new ArrayList<String>();
		userPathes.add(userPath);
		Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, userPathes);
		String jobname = "predictScoreOnce";
		if (pluginUtil.isDebug())
		{
			jobname = String.format("%sd%d", jobname, turn);
		}
		else {
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
		job.setNumReduceTasks(reduceNoBig);
		FileInputFormat.setInputPaths(job, new Path(execuUtil
				.getMROutSliptFiles(authorSimWorkPath.getPathValue())));
		authorUserPredictOut = execuUtil.getWriteOutputForTurns(turn, 
				pluginUtil.getPath(PluginUtil.TEMP_PREDICT_SCORE_PARTS_KEY)
				.getPathValue());
		FileOutputFormat.setOutputPath(job, new Path(authorUserPredictOut));
		execuUtil.checkOutputPath(authorUserPredictOut);
		if (job.waitForCompletion(true)) 
		{
			logUtil.getLogger().info(
					execuUtil.getJobExecuteLogstr(start, logStr, true));
			return 0;
		} 
		else {
			logUtil.getLogger().error(
					execuUtil.getJobExecuteLogstr(start, logStr, false));
			return 1;
		}
	}*/

}

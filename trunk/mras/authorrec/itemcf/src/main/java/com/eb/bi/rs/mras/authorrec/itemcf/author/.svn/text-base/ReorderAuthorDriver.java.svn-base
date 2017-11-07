package com.eb.bi.rs.mras.authorrec.itemcf.author;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.WrapDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReorderAuthorDriver extends WrapDriver {

    private String filterScoreKey = PluginUtil.TEMP_READ_USER_AUTHOR_SCORE_FILTER_KEY;
    private String filterScoreByClassifyTab = null;
    private String authorFilterKey = PluginUtil.TEMP_AUTHOR_AVG_SCORE_KEY;
    private PluginUtil.WorkPath authorFilterWorkPath = null;
    private String authorReindxKey = PluginUtil.TEMP_COLL_AUTHOR_REINDEX_KEY;
    private PluginUtil.WorkPath authorRedinxPath = null;
    private String reindexScoreKey = PluginUtil.TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT;
    private String reindexUserAuthorScorePath = null;
//	private int userSplitNO = 100;
//	private String incScoreFilterKey = PluginUtil.TEMP_INC_READ_USER_AUTHOR_FILTER_KEY;
//	private WorkPath incScoreFilterPath = null;

    public ReorderAuthorDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
//		userSplitNO = pluginUtil.getCollUserSplitNum();
        filterScoreByClassifyTab = pluginUtil.getPath(filterScoreKey).getPathValue();
        authorFilterWorkPath = pluginUtil.getPath(authorFilterKey);
        authorRedinxPath = pluginUtil.getPath(authorReindxKey);
        reindexUserAuthorScorePath = pluginUtil.getPath(reindexScoreKey).getPathValue();
        //	incScoreFilterPath = pluginUtil.getPath(incScoreFilterKey);
    }

    public String getReindxUserAuthorScorePath() {
        return reindexUserAuthorScorePath;
    }

    public int reorder()
            throws IOException, ClassNotFoundException, InterruptedException {
        int filterAuthorStatu = authorFilterPrepareJob();
        if (filterAuthorStatu != 0) {
            return filterAuthorStatu;
        }
        int reindexStatus = authorReindxPrepareJob();
        if (reindexStatus != 0) {
            return reindexStatus;
        }
        int filterScoreStatu = predictFilterPrepareJob();
        if (filterScoreStatu != 0) {
            return filterScoreStatu;
        }
        return 0;
    }

    /**
     * 协调过滤数据准备：只筛选打分用户数超过10的作者们;并计算平均分
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * msisdn|authorid|score
     * 输出: authorid
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int authorFilterPrepareJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not in book-recomm-table";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        conf.setInt(PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY,
                pluginUtil.getAuthorCommUserMin());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("authorFilterPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(AuthorFilterMapper.class);
        job.setReducerClass(AuthorFilterReducer.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job,
                new Path(execuUtil.getMROutSliptFiles(filterScoreByClassifyTab)));
        String out = authorFilterWorkPath.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(out));
        execuUtil.checkOutputPath(out);
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

    private int authorReindxPrepareJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        //作者重新编号，减少数据量
        long start = System.currentTimeMillis();
        String logStr = "reindx coll authors";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(authorFilterKey, authorFilterWorkPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("authorIndexPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(AuthorReindexMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(authorFilterWorkPath.getPathValue())));
        String out = authorRedinxPath.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(out));
        execuUtil.checkOutputPath(out);
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
     * 协调过滤数据准备：用户作者打分表中用编号的作者
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * msisdn|authorid|score
     * 输出: msisdn|authorid|score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int predictFilterPrepareJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not in book-recomm-table";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        //筛选后的作家
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(authorReindxKey, authorRedinxPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("predictFilterPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(CollaborativeInitFilterMapper.class);
//		job.setPartitionerClass(UserPartitioner.class);
        job.setNumReduceTasks(reduceNo);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,
                new Path(execuUtil.getMROutSliptFiles(filterScoreByClassifyTab)));
        FileOutputFormat.setOutputPath(job, new Path(reindexUserAuthorScorePath));
        execuUtil.checkOutputPath(reindexUserAuthorScorePath);
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
     * 平均分计算转移到authorFilterPrepareJob中
     * 协同过滤算法数据准备：计算一个作者打分的平均值
     * 输入：筛选后的用户作者打分表，predictAuthorFilterPrepareJob的结果
     *     msisdn|authorid|score
     * 输出: authorid|avgscore
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
/*	private int authorAvgScoreCalcuJob() 
            throws IOException, ClassNotFoundException, InterruptedException
	{
		String logStr = "calcu avg score of a read author";
		logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
		long start = System.currentTimeMillis();
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.setInt(PluginUtil.SIM_AUTHOR_COMM_USER_MIN_KEY, 
				pluginUtil.getAuthorCommUserMin());
		Job job = execuUtil.newJobAndAddCacheFile(conf, null);
		job.setJobName("authorAvgScore");
		job.setJarByClass(AuthorRecommDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(AuthorAvgScoreMapper.class);
		job.setReducerClass(AuthorAvgScoreReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(reduceNo);
		FileInputFormat.setInputPaths(job, 
				new Path(execuUtil.getMROutSliptFiles(filterScoreByClassifyTab)));
		String authorAvgScoreOut = authorFilterWorkPath.getPathValue();
		FileOutputFormat.setOutputPath(job, new Path(authorAvgScoreOut));
		execuUtil.checkOutputPath(authorAvgScoreOut);
		if (job.waitForCompletion(true)) 
		{
			logUtil.getLogger().info(
					execuUtil.getJobExecuteLogstr(start, logStr, true));
			return 0;
		} 
		else 
		{
			logUtil.getLogger().error(
					execuUtil.getJobExecuteLogstr(start, logStr, false));
			return 1;
		}
	}
	*/

    /**
     * 临时，以后移到predictFilterPrepareJob中
     * */
/*	private int userAuthorScoreSplitJob() 
			throws IOException, ClassNotFoundException, InterruptedException
	{
		String logStr = "split user score";
		logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
		long start = System.currentTimeMillis();
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
		Map<String, WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
		String key = PluginUtil.TEMP_COLL_AUTHOR_REINDEX_KEY;
		paraMap.put(key, pluginUtil.getPath(key));
		Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
		job.setJobName("authorScoreSplit");
		job.setJarByClass(AuthorRecommDriver.class);
		job.setMapperClass(UserAuthorScoreSplitMapper.class);
		job.setPartitionerClass(UserPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(100);
		String scorePath = execuUtil.getMROutSliptFiles(pluginUtil.getPath(
				PluginUtil.TEMP_READ_USER_AUTHOR_SCORE_FILTER_KEY)
				.getPathValue());
		FileInputFormat.setInputPaths(job, new Path(scorePath));
		String outPath = pluginUtil.getPath(PluginUtil
				.TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT).getPathValue();
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		execuUtil.checkOutputPath(outPath);
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

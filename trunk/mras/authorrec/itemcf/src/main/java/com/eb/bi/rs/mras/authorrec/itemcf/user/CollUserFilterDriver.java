package com.eb.bi.rs.mras.authorrec.itemcf.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.WrapDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.order.UserPrefClassSplitMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitioner;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CollUserFilterDriver extends WrapDriver {

    private String collDeepUserKey = PluginUtil.TEMP_DEEP_USER_GROUP_KEY;
    private PluginUtil.WorkPath collDeepUserPath = null;
    private String collDeepUser6cmKey = PluginUtil.TEMP_DEEP_USER_GROUP_6CM_KEY;
    private PluginUtil.WorkPath collDeepUser6cmPath = null;
    private String filterScoreKey = PluginUtil.TEMP_READ_USER_AUTHOR_SCORE_FILTER_KEY;
    private String filterScoreByClassifyTab = null;
    private String prefSplitKey = PluginUtil.TEMP_USER_PREF_CLASS_SPLIT_OUT;
    private PluginUtil.WorkPath prefSplitPath = null;
    private int userSplitNO = 100;

    public CollUserFilterDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
        collDeepUserPath = pluginUtil.getPath(collDeepUserKey);
        collDeepUser6cmPath = pluginUtil.getPath(collDeepUser6cmKey);
        filterScoreByClassifyTab = pluginUtil.getPath(filterScoreKey).getPathValue();
        prefSplitPath = pluginUtil.getPath(prefSplitKey);
        userSplitNO = pluginUtil.getCollUserSplitNum();
    }

    public int filterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        int userGroupStatu = deepUserJob();
        if (userGroupStatu != 0) {
            return userGroupStatu;
        }
        int user6cmStatu = deepUser6cmJob();
        if (user6cmStatu != 0) {
            return user6cmStatu;
        }
        int scoreFilterStatu = scoreFilterPrepareJob();
        if (scoreFilterStatu != 0) {
            return scoreFilterStatu;
        }
        int prefSplitStatu = userPrefSplitJob();
        if (prefSplitStatu != 0) {
            return prefSplitStatu;
        }
        return 0;
    }

    public String getPrefSplitPath() {
        return prefSplitPath.getPathValue();
    }

    /**
     * 协调过滤数据准备：只保留深度用户
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * msisdn|authorid|score
     * 输出: authorid
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int deepUserJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not deep group";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");

        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("deeplUserFilter");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(CollDeepUserFilterMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(reduceNo);

        String userGroupPath = pluginUtil.getPath(PluginUtil.USER_GROUP_KEY).getPathValue();
        String out = collDeepUserPath.getPathValue();
        execuUtil.checkOutputPath(out);

        FileInputFormat.setInputPaths(job, new Path(userGroupPath));
        FileOutputFormat.setOutputPath(job, new Path(out));

        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }

    /**
     * 协调过滤数据准备：只保留6cm表中的深度用户
     * 输入：6cm表
     * msisdn|bookid|depth
     * 输出: msisdn
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int deepUser6cmJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not deep group";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(collDeepUserKey, collDeepUserPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("userFilterPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(CollDeep6cmUserFilterMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(reduceNo);
        String read6cmPath = pluginUtil.getPath(PluginUtil.USER_READ_DEPTH_6CM_KEY).getPathValue();
        String inputUserPath = read6cmPath;
/*		String toRecommPath = pluginUtil.getPath(
                PluginUtil.TORECOMM_USER_INC_KEY).getPathValue();
		if (pluginUtil.isIncrementUpdate())
		{
			inputUserPath = toRecommPath;
		}*/
        FileInputFormat.setInputPaths(job, new Path(inputUserPath));
        String out = collDeepUser6cmPath.getPathValue();
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
     * 协调过滤数据准备：只筛选作家分类表中的作者们;剔除得分小于0。01的记录;只筛选深度用户
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * msisdn|authorid|score
     * 输出: msisdn|authorid|score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int scoreFilterPrepareJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not in book-recomm-table";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        //筛选后的作家
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(collDeepUser6cmKey, collDeepUser6cmPath);
        String key = PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("scoreFilterPrepare");
        job.setNumReduceTasks(reduceNoBig);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(ScoreFilterMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        String scorePathOut = pluginUtil.getPath(
                PluginUtil.READ_AUTHOR_SCORE_OUT_KEY).getPathValue();
        FileInputFormat.setInputPaths(job,
                new Path(execuUtil.getMROutSliptFiles(scorePathOut)));
        FileOutputFormat.setOutputPath(job, new Path(filterScoreByClassifyTab));
        execuUtil.checkOutputPath(filterScoreByClassifyTab);
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
     * 用户偏好表分片，只保留6cm表中的深度用户
     * 输入：用户偏好表
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int userPrefSplitJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "split user prefer class";
        long start = System.currentTimeMillis();
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(collDeepUser6cmKey, collDeepUser6cmPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("splitUserPrefClass");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(UserPrefClassSplitMapper.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setNumReduceTasks(userSplitNO);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(pluginUtil
                .getPath(PluginUtil.USER_PREFER_CLASS_KEY).getPathValue()));
        String out = prefSplitPath.getPathValue();
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
     * 协调过滤数据准备：筛选待更新的用户
     * 输入：msisdn|aid|score
     * 输出: msisdn|aid|score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int filterIncUserSplit(String origUserAuthorScorePath,
                                   int turn, String splitPath, String splitTemp)
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not inc";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.TORECOMM_USER_INC_KEY;
        PluginUtil.WorkPath wpath = pluginUtil.getPath(key);
        wpath.setIsVirtual(true);
        paraMap.put(key, wpath);
        List<String> pathes = new ArrayList<String>();
        pathes.add(splitPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, pathes);
        String jobname = String.format("userFilterPrepare%d", turn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(SplitIncUserMapper.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(reduceNo);
        FileInputFormat.setInputPaths(job, new Path(origUserAuthorScorePath));
        String out = execuUtil.getWriteOutputForTurns(turn, splitTemp);
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

    private int incUserFilterJob(String scorePath, String splitTemp)
            throws IOException, InterruptedException, ClassNotFoundException {
        String key = PluginUtil.TORECOMM_USER_INC_KEY;
        String incPath = pluginUtil.getPath(key).getPathValue();
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] starts = fs.globStatus(new Path(incPath));
        for (int i = 0; i < starts.length; i++) {
            String incSplit = starts[i].getPath().toString();
            int ret = filterIncUserSplit(scorePath, i, incSplit, splitTemp);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    /**
     * 若是初始化，用6cm中的全部用户，reorder
     * 否则若是增量更新，过滤出待计算的用户，并reorder
     *
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws java.io.IOException
     */
    public String predictUserPrepare(String origUserAuthorScorePath)
            throws IOException, ClassNotFoundException, InterruptedException {
        String outPath = pluginUtil.getPath(PluginUtil
                .TEMP_COLL_PREDICT_FILTER_SCORE_OUT).getPathValue();
        String authorScorePath = null;
        if (pluginUtil.isIncrementUpdate()) {
            String splitTempOut = pluginUtil.getPath(PluginUtil
                    .TEMP_INC_READ_USER_AUTHOR_FILTER_KEY).getPathValue();
            int filter = incUserFilterJob(origUserAuthorScorePath, splitTempOut);
            if (filter != 0) {
                return null;
            }
            authorScorePath = execuUtil.getPartsMROutSliptFiles(splitTempOut);
        } else {
            authorScorePath = origUserAuthorScorePath;
        }
        int ret = reorderUserSplit(authorScorePath, outPath);
        if (ret == 0) {
            return outPath;
        }
        return null;
    }


    private int reorderUserSplit(String authorScorePath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "filter author not inc";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("reorder");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(ReorderUserMapper.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(userSplitNO);
        FileInputFormat.setInputPaths(job, new Path(authorScorePath));
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
}

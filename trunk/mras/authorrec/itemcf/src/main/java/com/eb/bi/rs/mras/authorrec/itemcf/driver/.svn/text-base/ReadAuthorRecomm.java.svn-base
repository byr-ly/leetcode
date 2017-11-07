package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.partition.UserPartitioner;
import com.eb.bi.rs.mras.authorrec.itemcf.read.DeepReadDepthFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.read.ReadAuthorToRecommFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.read.ReadRecommendMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.read.ReadRecommendReducer;
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

public class ReadAuthorRecomm extends CommRecommDriver {

    //input
    private String readUserAuthorScorePath = null;
    //output
    private String torecommUserAuthorKey = PluginUtil.TEMP_TORECOMM_USER_READ_KEY;
    private PluginUtil.WorkPath torecommUserAuthorOut = null;
    private String deepReadFilterKey = PluginUtil.TEMP_DEEP_READ_FILTER_READ_KEY;
    private PluginUtil.WorkPath deepReadFilterOut = null;

    public ReadAuthorRecomm(Configuration cf, String initPath) {
        super(cf, initPath);
        this.readUserAuthorScorePath = pluginUtil.getPath(
                PluginUtil.READ_AUTHOR_SCORE_OUT_KEY).getPathValue();
        this.torecommUserAuthorOut = pluginUtil.getPath(torecommUserAuthorKey);
        this.deepReadFilterOut = pluginUtil.getPath(deepReadFilterKey);
        this.recommOutPath = pluginUtil.getPath(
                PluginUtil.TEMP_RECOMM_READ_RESULT_KEY).getPathValue();
    }

    public ReadAuthorRecomm(Configuration cf, String initPath,
                            int turn, List<String> initParts) {
        super(cf, initPath, turn, initParts);
        this.readUserAuthorScorePath = pluginUtil.getPath(
                PluginUtil.READ_AUTHOR_SCORE_OUT_KEY).getPathValue();
        String path = execuUtil.getWriteOutputForTurns(currTurn,
                pluginUtil.getPath(torecommUserAuthorKey).getPathValue());
        this.torecommUserAuthorOut = pluginUtil.new WorkPath(path, true, true);
        path = execuUtil.getWriteOutputForTurns(currTurn,
                pluginUtil.getPath(deepReadFilterKey).getPathValue());
        this.deepReadFilterOut = pluginUtil.new WorkPath(path, true, true);
        this.recommOutPath = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil
                .getPath(PluginUtil.TEMP_RECOMM_READ_RESULT_KEY).getPathValue());
    }

    /**
     * 筛选待推荐用户
     * 输入：用户-作者打分表
     * 格式：msisdn|authorid|score
     * 过滤：score<4，且用户不在指定待推荐用户库中的数据
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
        String logStr = "filter user for read recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        PluginUtil.WorkPath workPath = pluginUtil.new WorkPath(this.initUserPath, false, true);
        if (currTurn >= 0) {
            workPath.setIsVirtual(true);
        }
        paraMap.put("initUserPath", workPath);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, partFilePathes);
        String jobName = String.format("toRecommUserFilter%d", currTurn);
        job.setJobName(jobName);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ReadAuthorToRecommFilterMapper.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(
                execuUtil.getMROutSliptFiles(this.readUserAuthorScorePath)));
        String torecommPath = torecommUserAuthorOut.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(torecommPath));
        execuUtil.checkOutputPath(torecommPath);
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
     * 筛选智能推荐基础数据近6月累计表，以减少数据量
     * 输入：智能推荐基础数据近6月累计表
     * 格式：msisdn|bookid
     * 过滤：用户-作者不在toRecommUserFilterJob结果中；只记录深度和超深度阅读记录
     * 输出: msisdn|bookid|read_depth
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int toRecommBooksFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "filter books for read recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(torecommUserAuthorKey, torecommUserAuthorOut);
        String key = PluginUtil.BOOKINFO_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobName = String.format("toRecommBooksFilter%d", currTurn);
        job.setJobName(jobName);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(DeepReadDepthFilterMapper.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        if (currTurn >= 0 && readDepthPartPathes != null) {
            for (String s : readDepthPartPathes) {
                FileInputFormat.addInputPath(job, new Path(s));
            }
        } else {
            String readPath = pluginUtil.getPath(PluginUtil
                    .TEMP_REORDER_READ_DEPTH_KEY).getPathValue();
            FileInputFormat.setInputPaths(job, new Path(
                    execuUtil.getMROutSliptFiles(readPath)));
        }
        String deepreadPath = deepReadFilterOut.getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(deepreadPath));
        execuUtil.checkOutputPath(deepreadPath);
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
     * 打分作者推荐：根据用户-作者打分表，按照作家打分高低选排名前10的作家入库。
     * 筛选图书规则：图书分类与该作家所写图书本数最多的分类相一致；typeid=1&上架；用户6个月未访问或未阅读或浅度阅读，
     * 输入：用户作者打分表，readAuthorScoreJob的结果
     * msisdn|authorid|score
     * 输出: msisdn|authorid|bookid|score|type
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int orderRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "recomm read author";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(deepReadFilterKey, deepReadFilterOut);
        String key = PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        key = PluginUtil.BOOKINFO_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobName = String.format("recommReadAuthor%d", currTurn);
        job.setJobName(jobName);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ReadRecommendMapper.class);
        job.setReducerClass(ReadRecommendReducer.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(this.torecommUserAuthorOut.getPathValue())));
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

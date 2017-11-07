package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.filler.*;
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


public class FillerAuthorRecomm extends CommRecommDriver {

    private PluginUtil.WorkPath torecommUserPrefFilterPath = null;
    private String torecommFilterkey = PluginUtil.TEMP_TORECOMM_USER_FILLER_KEY;
    private PluginUtil.WorkPath deepReadFilterOut = null;
    private String deepReadFilterKey = PluginUtil.TEMP_DEEP_READ_FILTER_FILLER_KEY;
    private String famousAuthorBookKey = PluginUtil.TEMP_AUTHOR_FAMOUS_BOOK_KEY;
    private PluginUtil.WorkPath famousAuthorBookOut = null;
    private String noFillerUserKey = PluginUtil.TEMP_NO_FILLER_USER_KEY;

    public FillerAuthorRecomm(Configuration cf, String initPath) {
        super(cf, initPath);
        // TODO Auto-generated constructor stub
        this.torecommUserPrefFilterPath = pluginUtil.getPath(torecommFilterkey);
        this.deepReadFilterOut = pluginUtil.getPath(deepReadFilterKey);
        this.famousAuthorBookOut = pluginUtil.getPath(famousAuthorBookKey);
        this.recommOutPath = pluginUtil.getPath(PluginUtil.TEMP_RECOMM_FILLER_RESULT_KEY).getPathValue();
        this.userPrefFilterPath = pluginUtil.getPath(userPrefFilterKey);
    }

    public FillerAuthorRecomm(Configuration cf, String initPath,
                              int turn, List<String> initParts) {
        super(cf, initPath, turn, initParts);
        String path = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.getPath(torecommFilterkey).getPathValue());
        this.torecommUserPrefFilterPath = pluginUtil.new WorkPath(path, true, true);
        path = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.getPath(deepReadFilterKey).getPathValue());
        this.deepReadFilterOut = pluginUtil.new WorkPath(path, true, true);
        this.famousAuthorBookOut = pluginUtil.getPath(famousAuthorBookKey);
        this.recommOutPath = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.getPath(PluginUtil.TEMP_RECOMM_FILLER_RESULT_KEY).getPathValue());
        path = execuUtil.getWriteOutputForTurns(currTurn, pluginUtil.getPath(userPrefFilterKey).getPathValue());
        this.userPrefFilterPath = pluginUtil.new WorkPath(path, true, true);
    }

    public String getPrefSplitPath() {
        return this.userPrefFilterPath.getPathValue();
    }

    /**
     * 从用户偏好表中筛选待推荐用户
     * 输入：用户偏好表
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int toRecommUserPrefFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "filter user prefer class for predict recommend";
        long start = System.currentTimeMillis();
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
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
        String jobName = String.format("fillerUserPrefFilter%d", currTurn);
        job.setJobName(jobName);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ToRecommFilterMapper.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(pluginUtil
                .getPath(PluginUtil.USER_PREFER_CLASS_KEY).getPathValue()));
        String out = userPrefFilterPath.getPathValue();
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
     * 筛选待推荐用户
     * 输入：指定待推荐用户库
     * 格式：msisdn
     * 过滤：预测推荐结果表中存在的用户
     * 关联：偏好表
     * 输出: msisdn|偏好|偏好classid
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int toRecommUserFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        int filterPrefUserToRecommStatu = toRecommUserPrefFilterJob();
        if (filterPrefUserToRecommStatu != 0) {
            return filterPrefUserToRecommStatu;
        }

        String logStr = "filter user for filler recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(userPrefFilterKey, userPrefFilterPath);
        PluginUtil.WorkPath path = pluginUtil.getPath(noFillerUserKey);
        if (currTurn >= 0) {
            path.setIsVirtual(true);
        }
        paraMap.put(noFillerUserKey, path);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap, noFillerPathes);
        String jobname = String.format("fillerUserFilter%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(FillerUserFilterMapper.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        if (currTurn >= 0 && partFilePathes != null) {
            for (String s : partFilePathes) {
                FileInputFormat.addInputPath(job, new Path(s));
            }
        } else {
            FileInputFormat.setInputPaths(job, new Path(this.initUserPath));
        }
        String outPath = torecommUserPrefFilterPath.getPathValue();
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
     * 筛选智能推荐基础数据近6月累计表，以减少数据量
     * 输入：智能推荐基础数据近6月累计表
     * 格式：msisdn|bookid
     * 过滤：用户-作者不在toRecommUserFilterJob结果中；非深度和超深度阅读记录;作者为非推荐库关联用户,或非名家
     * 输出: msisdn|bookid
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int toRecommBooksFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "filter books for filler recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(torecommFilterkey, torecommUserPrefFilterPath);
        paraMap.put(famousAuthorBookKey, famousAuthorBookOut);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobname = String.format("toRecommBooksFillerFilter%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(DeepReadDepthFillerFilterMapper.class);
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
        String outPath = deepReadFilterOut.getPathValue();
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
     * 补白用户作者推荐
     * 输入：
     * 关联：名家、用户偏好表、智能推荐数据近6个月
     * 输出: msisdn|authorid|bookid|score|type
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    protected int orderRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String logStr = "filler recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        conf.set("mapred.textoutputformat.separator", "|");
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        paraMap.put(deepReadFilterKey, deepReadFilterOut);
        paraMap.put(famousAuthorBookKey, famousAuthorBookOut);
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        String jobname = String.format("fillerRecommend%d", currTurn);
        job.setJobName(jobname);
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(FillerMapper.class);
        job.setReducerClass(FillerReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(torecommUserPrefFilterPath.getPathValue())));
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

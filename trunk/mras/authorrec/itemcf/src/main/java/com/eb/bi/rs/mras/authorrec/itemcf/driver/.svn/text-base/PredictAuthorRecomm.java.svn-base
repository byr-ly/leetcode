package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.filler.ToRecommFilterMapper;
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


public class PredictAuthorRecomm extends CommRecommDriver {

    private String predictUserAuthorScorePath = null;

    public PredictAuthorRecomm(Configuration cf, String initpath) {
        super(cf, initpath);
        predictUserAuthorScorePath = execuUtil.getPartsMROutSliptFiles(
                pluginUtil.getPath(PluginUtil
                        .TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue());
        recommOutPath = pluginUtil.getPath(PluginUtil
                .TEMP_RECOMM_PREDICT_RESULT_ORDER_KEY).getPathValue();
    }

    public PredictAuthorRecomm(Configuration cf, String initPath,
                               int turn, List<String> initParts) {
        super(cf, initPath, turn, initParts);
        predictUserAuthorScorePath = execuUtil.getPartsMROutSliptFiles(
                pluginUtil.getPath(PluginUtil
                        .TEMP_RECOMM_PREDILCT_RESULT_KEY).getPathValue());
        recommOutPath = execuUtil.getWriteOutputForTurns(currTurn,
                pluginUtil.getPath(PluginUtil.TEMP_RECOMM_PREDICT_RESULT_ORDER_KEY)
                        .getPathValue());
    }

    /**
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
        if (currTurn >= 0) {
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
        String predictPath = predictUserAuthorScorePath;
        FileInputFormat.setInputPaths(job, new Path(predictPath));
        FileOutputFormat.setOutputPath(job, new Path(this.recommOutPath));
        execuUtil.checkOutputPath(this.recommOutPath);
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

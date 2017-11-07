package com.eb.bi.rs.mras.authorrec.itemcf.module_test;

import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.filler.FamousAuthorBookFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LiMingji on 2015/12/22.
 */
public class FamousAuthorBooksFilterJobDriver extends Configured implements Tool {
    private PluginUtil pluginUtil = null;
    private LogUtil logUtil = null;
    private int reduceNo = 5;
    private int reduceNoBig = 20;
    private JobExecuUtil execuUtil = null;

    public FamousAuthorBooksFilterJobDriver(String conf, String dateStr) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.load(conf, dateStr);
        logUtil = LogUtil.getInstance();
        reduceNo = pluginUtil.getReduceNum();
        reduceNoBig = pluginUtil.getReduceNumBig();
        execuUtil = new JobExecuUtil();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: AuthorRecommDriver conf [date]");
            return;
        }
        String conf = args[0];
        Date begin = new Date();
        String dateStr = null;
        if (args.length > 1) {
            dateStr = args[1];
            if (dateStr.length() != 8) {
                System.err.println("date error. eg: 20150610");
                return;
            }
        }
        FamousAuthorBooksFilterJobDriver driver = new FamousAuthorBooksFilterJobDriver(conf, dateStr);
        int ret = ToolRunner.run(new Configuration(), driver, args);
        Date end = new Date();
        long timeCost = end.getTime() - begin.getTime();
        String timeInfo = String.format("time cost in total(s): %d", timeCost / 1000);
        driver.logUtil.getLogger().info(timeInfo);
        System.exit(ret);
    }

    /**
     * 从图书表中筛选名家——补白用
     * 输入：bookinfo
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    public int run(String[] args) throws Exception {
        String logStr = "filter books of famous authors for filler recommend";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());

        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.TEMP_AUTHOR_CLASSIFY_TABLE_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        key = PluginUtil.FAMOUS_AUTHOR_KEY;
        paraMap.put(key, pluginUtil.getPath(key));

        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("famousAuthorBooksFillerFilter");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(FamousAuthorBookFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        String bookinfoPath = pluginUtil.getPath(PluginUtil.BOOKINFO_KEY).getPathValue();
        String famousOut = pluginUtil.getPath(PluginUtil.TEMP_AUTHOR_FAMOUS_BOOK_KEY).getPathValue();
        execuUtil.checkOutputPath(famousOut);

        FileInputFormat.setInputPaths(job, new Path(bookinfoPath));
        FileOutputFormat.setOutputPath(job, new Path(famousOut));

        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }
}

package com.eb.bi.rs.mras.authorrec.itemcf.module_test;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.CounterWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.AuthorBigClassMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.AuthorBigClassReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

/**
 * Created by LiMingji on 2015/12/22.
 */
public class OrderAuthorBookCountOfAClassDriver extends Configured implements Tool {
    private PluginUtil pluginUtil = null;
    private LogUtil logUtil = null;
    private int reduceNo = 5;
    private int reduceNoBig = 20;
    private JobExecuUtil execuUtil = null;

    public OrderAuthorBookCountOfAClassDriver(String conf, String dateStr) {
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
        OrderAuthorBookCountOfAClassDriver driver = new OrderAuthorBookCountOfAClassDriver(conf, dateStr);
        int ret = ToolRunner.run(new Configuration(), driver, args);
        Date end = new Date();
        long timeCost = end.getTime() - begin.getTime();
        String timeInfo = String.format("time cost in total(s): %d", timeCost / 1000);
        driver.logUtil.getLogger().info(timeInfo);
        System.exit(ret);
    }

    /**
     * 根据作者所写各分类图书本数，对分类进行排序。最多记录5个分类
     * 输入：作者-分类-图书数量表，bookinfoDataPrepareJob的结果
     * authorid|classid|count
     * 输出: authorid|classid1|classid2|。。。
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    @Override
    public int run(String[] args) throws Exception {
        String logStr = "order class of an author according with book num";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);

        job.setJobName("orderAuthorBookCountOfAClass");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(AuthorBigClassMapper.class);
        job.setReducerClass(AuthorBigClassReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CounterWritable.class);

        String authorBookCountOutPath = pluginUtil.getPath(PluginUtil.TEMP_AUTHOR_CLASS_COUNT_KEY).getPathValue();
        String authorBigClassOutPath = pluginUtil.getPath(PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY).getPathValue();
        execuUtil.checkOutputPath(authorBigClassOutPath);

        FileInputFormat.setInputPaths(job, new Path(execuUtil.getMROutSliptFiles(authorBookCountOutPath)));
        FileOutputFormat.setOutputPath(job, new Path(authorBigClassOutPath));

        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }
}

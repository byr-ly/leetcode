package com.eb.bi.rs.mras.authorrec.itemcf.module_test;

import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookAllCountMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookCountReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookOnShelfCountMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.driver.AuthorRecommDriver;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * Created by LiMingji on 2015/12/11.
 */

/**
 * 数据准备：从bookinfo表计算作者各分类所写图书本数
 * 输入：dim.dim_bookinfo
 * bookid|authorid|classid|contentstatus|author_grade|typeid
 * 输出:key:authorid|classid; value:count
 */
public class BookInfoDataPreparedJobDriver extends Configured implements Tool {
    private PluginUtil pluginUtil = null;
    private LogUtil logUtil = null;
    private int reduceNo = 5;
    private int reduceNoBig = 20;
    private JobExecuUtil execuUtil = null;

    public BookInfoDataPreparedJobDriver(String conf, String dateStr) {
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
        BookInfoDataPreparedJobDriver driver = new BookInfoDataPreparedJobDriver(conf, dateStr);
        int ret = ToolRunner.run(new Configuration(), driver, args);
        Date end = new Date();
        long timeCost = end.getTime() - begin.getTime();
        String timeInfo = String.format("time cost in total(s): %d", timeCost / 1000);
        driver.logUtil.getLogger().info(timeInfo);
        System.exit(ret);
    }

    /**
     * 数据准备：从bookinfo表计算作者各分类所写图书本数
     * 输入：dim.dim_bookinfo
     * bookid|authorid|classid|contentstatus|author_grade|typeid
     * 输出:key:authorid|classid; value:count
     */
    @Override
    public int run(String[] strings) throws Exception {
        //筛选type=1的图书
        String logStr = "count books of author job";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        Job job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("bookinfoDataPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(AuthorBookAllCountMapper.class);
        job.setReducerClass(AuthorBookCountReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String bookAuthorInPath = pluginUtil.getPath(PluginUtil.BOOKINFO_KEY).getPathValue();
        String authorBookCountOutPath = pluginUtil.getPath(PluginUtil.TEMP_AUTHOR_CLASS_COUNT_KEY).getPathValue();
        execuUtil.checkOutputPath(authorBookCountOutPath);

        FileInputFormat.setInputPaths(job, new Path(bookAuthorInPath));
        FileOutputFormat.setOutputPath(job, new Path(authorBookCountOutPath));

        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }

        logUtil.getLogger().info("作者-分类-图书本数 开始计算");

        //筛选type=1,contentstatus=13的图书
        logStr = "count on shelf books of author job";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        job = execuUtil.newJobAndAddCacheFile(conf, null);
        job.setJobName("bookinfoOnShelfDataPrepare");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setMapperClass(AuthorBookOnShelfCountMapper.class);
        job.setReducerClass(AuthorBookCountReducer.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        authorBookCountOutPath = pluginUtil.getPath(PluginUtil.TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY).getPathValue();

        FileInputFormat.setInputPaths(job, new Path(bookAuthorInPath));
        FileOutputFormat.setOutputPath(job, new Path(authorBookCountOutPath));
        execuUtil.checkOutputPath(authorBookCountOutPath);

        logUtil.getLogger().info("作者-分类-上架图书本书 开始计算");

        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }
}

package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.CounterWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookAllCountMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookCountReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorBookOnShelfCountMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.bookinfo.AuthorClassifyTableMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.filler.FamousAuthorBookFilterMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.readscore.ReadAuthorScoreMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.readscore.ReadAuthorScoreReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.AuthorBigClassMapper;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.AuthorBigClassReducer;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AuthorRecommDriver extends Configured implements Tool {

    private PluginUtil pluginUtil = null;
    private LogUtil logUtil = null;
    private int reduceNo = 5;
    private int reduceNoBig = 20;
    private JobExecuUtil execuUtil = null;

    public AuthorRecommDriver(String conf, String dateStr) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.load(conf, dateStr);
        logUtil = LogUtil.getInstance();
        reduceNo = pluginUtil.getReduceNum();
        reduceNoBig = pluginUtil.getReduceNumBig();
        execuUtil = new JobExecuUtil();
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
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
                dateStr = null;
                System.err.println("date error. eg: 20150610");
                return;
            }
        }
        AuthorRecommDriver driver = new AuthorRecommDriver(conf, dateStr);
        int ret = ToolRunner.run(new Configuration(), driver, args);
        Date end = new Date();
        long timeCost = end.getTime() - begin.getTime();
        String timeInfo = String.format("time cost in total(s): %d",
                timeCost / 1000);
        driver.logUtil.getLogger().info(timeInfo);
        System.exit(ret);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        int bookinfoPrepareStatu = bookinfoDataPrepareJob();
        if (bookinfoPrepareStatu != 0) {
            return bookinfoPrepareStatu;
        }
        int readAuthorScoreStatus = readAuthorScoreJob();
        if (readAuthorScoreStatus != 0) {
            return readAuthorScoreStatus;
        }
        int authorClassifyStatu = authorClassifyTablePrepareJob();
        if (authorClassifyStatu != 0) {
            return authorClassifyStatu;
        }
        int orderAuthorClassStatu = orderAuthorBookCountOfAClassJob();
        if (orderAuthorClassStatu != 0) {
            return orderAuthorClassStatu;
        }
        int famousBookFilterStatu = famousAuthorBooksFilterJob();
        if (famousBookFilterStatu != 0) {
            return famousBookFilterStatu;
        }
        CoordinateFilterDriver coordinateDriver = new CoordinateFilterDriver(getConf());
        int coordinateFilterStatu = coordinateDriver.coordinateFilteringJob();
        if (coordinateFilterStatu != 0) {
            return coordinateFilterStatu;
        }
        RecommDisplayDriver displayDriver = new RecommDisplayDriver(getConf());
        int blacklistStatu = displayDriver.blacklistJob();
        if (blacklistStatu != 0) {
            return blacklistStatu;
        }
        RecommProduceDriver recommDriver = new RecommProduceDriver(getConf());
        int recommStatu = recommDriver.productRecommJob();
        if (recommStatu != 0) {
            return recommStatu;
        }
        if (pluginUtil.isDebug()) {
            int checkStatu = displayDriver.chechResultJob();
            if (checkStatu != 0) {
                return checkStatu;
            }
        }

        return 0;
    }

    /**
     * 数据准备：从bookinfo表计算作者各分类所写图书本数
     * 输入：dim.dim_bookinfo
     * bookid|authorid|classid|contentstatus|author_grade|typeid
     * 输出:key:authorid|classid; value:count
     */
    protected int bookinfoDataPrepareJob() throws
            IOException, ClassNotFoundException, InterruptedException {
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
        String bookAuthorInPath = pluginUtil
                .getPath(PluginUtil.BOOKINFO_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(bookAuthorInPath));
        String authorBookCountOutPath = pluginUtil
                .getPath(PluginUtil.TEMP_AUTHOR_CLASS_COUNT_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(authorBookCountOutPath));
        execuUtil.checkOutputPath(authorBookCountOutPath);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(
                    execuUtil.getJobExecuteLogstr(start, logStr, true));
        } else {
            logUtil.getLogger().error(
                    execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }

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
        FileInputFormat.setInputPaths(job, new Path(bookAuthorInPath));
        authorBookCountOutPath = pluginUtil.getPath(
                PluginUtil.TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(authorBookCountOutPath));
        execuUtil.checkOutputPath(authorBookCountOutPath);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(execuUtil.getJobExecuteLogstr(start, logStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(execuUtil.getJobExecuteLogstr(start, logStr, false));
            return 1;
        }
    }

    /**
     * 根据用户对图书打分计算用户对作者打分
     * 输入：用户图书打分结果表 dmn.irecm_us_bkid_score_all
     * msisdn|book_id|book_score
     * 过滤打分过低的图书
     * 输出:key:msisdn|authorid; value:score
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    protected int readAuthorScoreJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String jobLogStr = "calcu read author score";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(jobLogStr));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setFloat("bookScoreMin", (float) pluginUtil.getBookScoreMin());
        conf.setBoolean("bUseNewHadoop", pluginUtil.isNewHadoop());
        Map<String, PluginUtil.WorkPath> passKeyValues
                = new HashMap<String, PluginUtil.WorkPath>();
        //图书-作者
        String key = PluginUtil.BOOKINFO_KEY;
        passKeyValues.put(key, pluginUtil.getPath(key));
        //作者-分类-上架图书数量。bookinfoDataPrepareJob的结果
        key = PluginUtil.TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY;
        passKeyValues.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, passKeyValues);
        job.setJobName("readAuthorScore");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ReadAuthorScoreMapper.class);
        job.setReducerClass(ReadAuthorScoreReducer.class);
        job.setNumReduceTasks(reduceNoBig);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        String scorePath = pluginUtil
                .getPath(PluginUtil.USER_BOOK_SCORE_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(scorePath));
        String scoreOut = pluginUtil
                .getPath(PluginUtil.READ_AUTHOR_SCORE_OUT_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(scoreOut));
        execuUtil.checkOutputPath(scoreOut);
        if (job.waitForCompletion(true)) {
            logUtil.getLogger().info(
                    execuUtil.getJobExecuteLogstr(start, jobLogStr, true));
            return 0;
        } else {
            logUtil.getLogger().error(
                    execuUtil.getJobExecuteLogstr(start, jobLogStr, false));
            return 1;
        }
    }

    /**
     * 关联推荐库图书形式作者分类表
     * 输入：作者-分类-图书本数表
     * authorid|classid|count
     * 输出: authorid|classid|count
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int authorClassifyTablePrepareJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        String logStr = "recomm author table";
        logUtil.getLogger().info(execuUtil.beginJobLogStr(logStr));
        Configuration conf = new Configuration(getConf());
        conf.set("mapred.textoutputformat.separator", "|");
        conf.setBoolean(PluginUtil.USE_NEW_HADOOP_KEY, pluginUtil.isNewHadoop());
        //推荐库图书
        Map<String, PluginUtil.WorkPath> paraMap = new HashMap<String, PluginUtil.WorkPath>();
        String key = PluginUtil.RECOMM_BOOK_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        key = PluginUtil.BOOKINFO_KEY;
        paraMap.put(key, pluginUtil.getPath(key));
        Job job = execuUtil.newJobAndAddCacheFile(conf, paraMap);
        job.setJobName("authorClassifyTable");
        job.setJarByClass(AuthorRecommDriver.class);
        job.setNumReduceTasks(reduceNo);
        job.setMapperClass(AuthorClassifyTableMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        String authorCountOut = pluginUtil.getPath(
                PluginUtil.TEMP_AUTHOR_CLASS_COUNT_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(execuUtil
                .getMROutSliptFiles(authorCountOut)));
        String authorClassifyTablePath = pluginUtil.getPath(PluginUtil
                .TEMP_AUTHOR_CLASSIFY_TABLE_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(authorClassifyTablePath));
        execuUtil.checkOutputPath(authorClassifyTablePath);
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
     * 根据作者所写各分类图书本数，对分类进行排序。最多记录5个分类
     * 输入：作者-分类-图书数量表，bookinfoDataPrepareJob的结果
     * authorid|classid|count
     * 输出: authorid|classid1|classid2|。。。
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int orderAuthorBookCountOfAClassJob()
            throws ClassNotFoundException, IOException, InterruptedException {
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
        String authorBookCountOutPath = pluginUtil.getPath(
                PluginUtil.TEMP_AUTHOR_CLASS_COUNT_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(
                execuUtil.getMROutSliptFiles(authorBookCountOutPath)));
        String authorBigClassOutPath = pluginUtil.getPath(
                PluginUtil.TEMP_AUTHOR_BIG_CLASS_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(authorBigClassOutPath));
        execuUtil.checkOutputPath(authorBigClassOutPath);
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
     * 从图书表中筛选名家——补白用
     * 输入：bookinfo
     * 输出：
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int famousAuthorBooksFilterJob()
            throws IOException, InterruptedException, ClassNotFoundException {
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
        String bookinfoPath = pluginUtil.getPath(
                PluginUtil.BOOKINFO_KEY).getPathValue();
        FileInputFormat.setInputPaths(job, new Path(bookinfoPath));
        String famousOut = pluginUtil
                .getPath(PluginUtil.TEMP_AUTHOR_FAMOUS_BOOK_KEY).getPathValue();
        FileOutputFormat.setOutputPath(job, new Path(famousOut));
        execuUtil.checkOutputPath(famousOut);
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

package booklistrec;

import booklistrec.booklist_choose.BookListChooseMapper;
import booklistrec.booklist_choose.BookListChooseReducer;
import booklistrec.booklist_filter.BookListFilterMapper;
import booklistrec.booklist_filter.BookListFilterReducer;
import booklistrec.booklist_score.BookListScoreMapper;
import booklistrec.booklist_score.BookListScoreReducer;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by liyang on 2016/5/12.
 */
public class BookListRecomDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public BookListRecomDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new BookListRecomDriver(args), args);

        Date end = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String endTime = format.format(end);
        long timeCost = end.getTime() - begin.getTime();

        PluginResult result = pluginUtil.getResult();
        result.setParam("endTime", endTime);
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(s): " + (timeCost / 1000.0));
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        PluginConfig config = pluginUtil.getConfig();
        Configuration conf;
        Job job;
        FileStatus[] fs;
        long start;

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径：用户历史偏好表。
         ** 输出：
         **		书单的得分。
         ** 功能描述：
         ** 	根据分类偏好和标签对用户进行划分，然后计算书单得分。
         **
         **
         *******************************************************************************************/

        log.info("start to compute the score of booksheet...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        String inputPath = config.getParam("user_input_path", "liyang/booklistrec/input");
        String recOutputDir = config.getParam("sheet_score", "liyang/booklistrec/middle/sheet_score");
        String prefBookInfo = config.getParam("booklist_desc_cache_path", "liyang/booklistrec/cache/book_list_desc_new");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("书单描述信息路径: " + prefBookInfo);

        //书单信息
        fs = FileSystem.get(conf).globStatus(new Path(prefBookInfo));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }
        job = new Job(conf);
        job.setJarByClass(BookListRecomDriver.class);
        job.setJobName("BookListScore");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(BookListScoreMapper.class);
        job.setReducerClass(BookListScoreReducer.class);

        job.setNumReduceTasks(config.getParam("book_list_score_reduce_task_num", 1));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		1.输入路径：书单得分表。
         ** 缓存：
         **     书单图书表
         ** 输出：
         **		书单最终得分。
         ** 功能描述：
         ** 	根据用户历史阅读计算书单最终得分，同时进行书单过滤。
         **
         **
         *******************************************************************************************/

        log.info("start to filter the booksheet...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        inputPath = config.getParam("sheet_score", "liyang/booklistrec/middle/sheet_score");
        recOutputDir = config.getParam("sheet_final_score_output_path", "liyang/booklistrec/middle/sheet_final_score");
        prefBookInfo = config.getParam("sheet_book_cache_path", "liyang/booklistrec/cache/sheetbook/book_list_book");
        String hisReadInfo = config.getParam("history_read", "liyang/booklistrec/cache/yingwj.txt");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("书单图书路径: " + prefBookInfo);
        System.out.println("用户历史阅读路径: " + hisReadInfo);

        //图书信息
        fs = FileSystem.get(conf).globStatus(new Path(prefBookInfo));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf);
        job.setJarByClass(BookListRecomDriver.class);
        job.setJobName("BookListChoose");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(BookListChooseMapper.class);
        job.setReducerClass(BookListChooseReducer.class);

        job.setNumReduceTasks(config.getParam("book_list_filter_reduce_task_num", 1));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, BookListChooseMapper.class);
        MultipleInputs.addInputPath(job, new Path(hisReadInfo),TextInputFormat.class,BookListChooseMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径： 书单分数列表。
         ** 输出：
         **		书单待推荐列表。
         ** 功能描述：
         ** 	将过滤完的书单按得分降序排列，取排名前10的书单。
         **
         **
         *******************************************************************************************/

        log.info("start to generate the recommendation list...");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());

        inputPath = config.getParam("sheet_final_score_output_path", "liyang/booklistrec/middle/sheet_final_score");
        recOutputDir = config.getParam("sort_score_output_path", "liyang/booklistrec/output/book_list_sort");
        prefBookInfo = config.getParam("booklist_desc_cache_path", "liyang/booklistrec/cache/booklist/booklistinfo");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);
        System.out.println("书单描述信息路径: " + prefBookInfo);

        //书单描述信息
        fs = FileSystem.get(conf).globStatus(new Path(prefBookInfo));

        for (int i = 0; i < fs.length; i++) {
            DistributedCache.addCacheFile(URI.create(fs[i].getPath().toString()), conf);
            System.out.println(fs[i].getPath().toString() + " has been add into distributedCache");
        }

        job = new Job(conf);
        job.setJarByClass(BookListRecomDriver.class);
        job.setJobName("BookListFilter");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(BookListFilterMapper.class);
        job.setReducerClass(BookListFilterReducer.class);

        job.setNumReduceTasks(config.getParam("c", 1));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));
        check(recOutputDir);

        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        return 0;
    }

    public void check(String fileName) {
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {	//if exists, delete
                boolean isDel = fs.delete(f,true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

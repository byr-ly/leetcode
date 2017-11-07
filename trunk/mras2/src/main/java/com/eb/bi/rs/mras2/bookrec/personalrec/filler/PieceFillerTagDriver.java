package com.eb.bi.rs.mras2.bookrec.personalrec.filler;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class PieceFillerTagDriver extends BaseDriver {
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Logger log = Logger.getLogger("PieceFillerTagDriver");
        long start = System.currentTimeMillis();
        Job job = null;
        Configuration conf;
        conf = new Configuration(getConf());

        Properties app_conf = super.properties;
        //配置加载--------------------------------------------------
        //目录配置
        String inputPath = app_conf.getProperty("hdfs.input.path");
        String outputPath = app_conf.getProperty("hdfs.output.path");

        String cachePath = app_conf.getProperty("hdfs.cache.path");
        //并行度配置
        //int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));
        int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
        //应用配置
        String recommendNum = app_conf.getProperty("Appconf.piecefiller.recommendnum", "12");
        String randomNum = app_conf.getProperty("Appconf.piecefiller.randomnum", "500");
        //--------------------------------------------------------
        //并行度配置
        conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
        //应用配置
        conf.set("Appconf.piecefiller.recommendnum", recommendNum);
        conf.set("Appconf.piecefiller.randomnum", randomNum);
        //--------------------------------------------------------
        job = new Job(conf);

        //补白库加载
        FileSystem fs1 = FileSystem.get(conf);
        FileStatus[] status1 = fs1.globStatus(new Path(cachePath));
        for (int i = 0; i < status1.length; i++) {
            //DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()), conf);
        	job.addCacheFile(URI.create(status1[i].getPath().toString()));
            log.info("book_fillter_data file: " + status1[i].getPath().toString() + " has been add into distributed cache");
        }
        //job-setup
        job.setJarByClass(PieceFillerTagDriver.class);
        //检查输出目录是否存在
        check(outputPath, conf);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        //设置输出地址
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //设置M-R
        job.setMapperClass(PieceFillerTagMapper.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(PieceFillerTagReducer.class);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //日志==================================================================================
        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        log.info("=================================================================================");

        return 0;
    }

    public void check(String path, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class PieceFillerTagMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context ctx)
                throws IOException, InterruptedException {
            //图书|标签
            String[] fields = value.toString().split("\\|");

            if (fields.length != 2) {
                return;
            }

            ctx.write(new Text(fields[1]), new Text(fields[0]));
        }
    }

    public static class PieceFillerTagReducer extends Reducer<Text, Text, NullWritable, Text> {
        //补白图书库
        private Set<String> fillterList = new HashSet<String>();

        private List<String> resultList = new ArrayList<String>();

        private int recommendNum = 0;
        private int randomNum = 0;

        @Override
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            recommendNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.recommendnum"));
            randomNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.randomnum"));

            super.setup(context);
//			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            URI[] localFiles = context.getCacheFiles();

            for (int i = 0; i < localFiles.length; i++) {
                String line;
                BufferedReader in = null;
                try {
                    Path path = new Path(localFiles[i].getPath());
                    in = new BufferedReader(new FileReader(path.getName().toString()));
                    while ((line = in.readLine()) != null) {
                        fillterList.add(line);
                    }
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            //key:标签;val:图书
            Set<String> bookSet = new HashSet<String>();

            for (Text value : values) {
                //只要补白库中的图书
                if (!fillterList.contains(value.toString())) {
                    continue;
                }
                bookSet.add(value.toString());
            }

            if (bookSet.size() < 5) {
                return;
            }

            String books = "";

            for (String onebook : bookSet) {
                books = books + onebook + "|";
            }
            //标签|图书集
            resultList.add(key.toString() + "|" + books);
        }

        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            Set<String> backupOutput = new HashSet<String>();

            for (int i = 0; i != randomNum; i++) {
                //用于随机标签结果
                List<String> tagResult = new ArrayList<String>();
                //已出图书列表
                Set<String> bookSet = new HashSet<String>();
                //输出结果
                Set<String> output = new HashSet<String>();
                //每个标签里随机出4本书
                for (int j = 0; j != resultList.size(); j++) {
                    String[] fields = resultList.get(j).split("\\|");
                    int hasbook = 0;
                    for (int k = 1; k != fields.length; k++) {
                        if (!bookSet.contains(fields[k])) {
                            hasbook++;
                        }
                    }
                    if (hasbook < 4) {
                        continue;
                    }
                    hasbook = 0;
                    String tagbookString = fields[0] + "|";
                    for (; ; ) {
                        //随机一个图书
                        int random = new Random().nextInt(fields.length);
                        if (random == 0) {
                            random = 1;
                        }
                        if (!bookSet.contains(fields[random])) {
                            bookSet.add(fields[random]);
                            tagbookString = tagbookString + fields[random] + "|";
                            hasbook++;
                        }
                        if (hasbook == 4) {
                            break;
                        }
                    }
                    tagResult.add(tagbookString);
                }
                //本次随机取书成功
                if (tagResult.size() >= 4) {
                    for (; ; ) {
                        int random = new Random().nextInt(tagResult.size());

                        output.add(tagResult.get(random));

                        if (output.size() == recommendNum) {
                            break;
                        }
                    }
                    Iterator<String> it = output.iterator();
                    while (it.hasNext()) {
                        String entry = it.next();
                        entry = entry.substring(0, entry.length() - 1);
                        //写结果
                        context.write(NullWritable.get(), new Text(i + "|" + entry));
                        if (backupOutput.size() < 4) {
                            backupOutput.add(entry);
                        }
                    }
                } else {//本次随机取书没有成功//使用备用结果
                    if (i == 0) {
                        i--;
                        continue;
                    }
                    Iterator<String> it = backupOutput.iterator();
                    while (it.hasNext()) {
                        String entry = it.next();
                        //写结果
                        context.write(NullWritable.get(), new Text(i + "|" + entry));
                    }
                }
            }
        }
    }
}

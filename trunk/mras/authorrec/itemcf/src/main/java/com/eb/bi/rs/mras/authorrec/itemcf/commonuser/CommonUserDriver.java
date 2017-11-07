package com.eb.bi.rs.mras.authorrec.itemcf.commonuser;

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

/**
 * Created by liyang on 2016/3/8.
 */
public class CommonUserDriver extends Configured implements Tool{
//    Logger log = PluginUtil.getInstance().getLogger();

    public static void main( String[] args ) throws Exception{
        int ret = ToolRunner.run(new CommonUserDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {
//        PluginConfig config = PluginUtil.getInstance().getConfig();
        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径： 用户对作家的打分。
         ** 输出：
         **		作家之间的共现用户列表
         ** 功能描述：
         ** 	输出作家之间的共现用户列表。
         **
         **
         *******************************************************************************************/

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration(getConf());

//        String inputPath = "liyang/output/author_origin_score";
        String inputPath = "liyang/input/authorscore";
        String recOutputDir = "liyang/output/commom_user";

//        String inputPath = config.getParam("author_score_output_path", "/home/recsys/data/recsys_data/output/author_origin_score");
//        String recOutputDir = config.getParam("common_user_output_path", "/home/recsys/data/recsys_data/output/common_user");

        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + recOutputDir);

        Job job = new Job(conf);
        job.setJarByClass(CommonUserDriver.class);
        job.setJobName("CommonUser");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(CommomUserMapper.class);
        job.setReducerClass(CommonUserReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(recOutputDir));

        if (job.waitForCompletion(true)) {
//            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 0;
        } else {
//            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
//        return 0;
    }
}

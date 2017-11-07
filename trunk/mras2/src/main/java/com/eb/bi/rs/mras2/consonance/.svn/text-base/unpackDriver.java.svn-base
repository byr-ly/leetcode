package com.eb.bi.rs.mras2.consonance;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**熵值法求用户品牌得分
 * Created by linwanying on 2016/11/16.
 */
public class unpackDriver extends Configured implements Tool {

    private static PluginUtil pluginUtil;
    private static Logger log;

    public unpackDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }
    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new unpackDriver(args), args);

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
        Logger log = Logger.getLogger("EntropyDriver");
        PluginConfig config = pluginUtil.getConfig();
        Configuration conf;
        Job job;
        long start;

        //拆分熵值评分
        log.info("========start to run unpack job========");
        start = System.currentTimeMillis();
        conf = new Configuration(getConf());
        job = Job.getInstance(conf, "unpack job");
        job.setJarByClass(unpackDriver.class);
        //M-R
        job.setMapperClass(unpackMapper.class);
        job.setNumReduceTasks(0);
        //设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置输入地址
        FileInputFormat.setInputPaths(job, new Path(config.getParam("entropy.unpack.input", "/user/ebupt/linwanying/anhui/entropy/originalinfo")));
        //设置输出地址
        String peakDir = config.getParam("entropy.unpack.output", "/user/ebupt/linwanying/anhui/entropy/peak");
        FileOutputFormat.setOutputPath(job, new Path(peakDir));
        check(peakDir);
        if (job.waitForCompletion(true)) {
            log.info("generate job peak complete, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
        } else {
            log.error("generate job peak failed, time cost: " + (System.currentTimeMillis() - start)/1000 + "s");
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

package com.eb.bi.rs.andedu.inforec.filler_infos;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;

public class FillerInfosDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		PluginUtil.getInstance().init(args);
		Logger log = PluginUtil.getInstance().getLogger();
		Date dateBeg = new Date();

		int ret = ToolRunner.run(new FillerInfosDriver(), args);

		Date dateEnd = new Date();
		long timeCost = dateEnd.getTime() - dateBeg.getTime();

		PluginResult result = PluginUtil.getInstance().getResult();
		result.setParam("endTime",
				new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC
				: PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully"
				: "run failed.");
		result.save();

		log.info("time cost in total(ms) :" + timeCost);
		System.exit(ret);
	}

	@Override
	public int run(String[] strings) throws Exception {
		Logger log = PluginUtil.getInstance().getLogger();
		PluginConfig config = PluginUtil.getInstance().getConfig();
		/**
         * ********************************************************************************************
         * MAP REDUCE JOB:
		 ** 输入：
		 ** 		和新闻新闻数据。资讯idchr(001)资讯标题chr(001)图片封面地址chr(001)来源chr(001)浏览量
		 ** 		chr(001)生成的静态页面的URL地址chr(001)发布时间yyyymmddhhMMsschr(001)省份id,多个用逗号隔开
		 ** 		chr(001)栏目id(chr(001)为分隔符) 
		 ** 输出： 
		 ** 		id-排序资讯 id 资讯id|资讯id|资讯id... 
		 ** 功能描述：
		 ** 		读入所有新闻所有内容，选出最新最热资讯。
		 ** 
		 ** 
		 *******************************************************************************************/
		FileStatus[] status;
		long start = System.currentTimeMillis();
		Configuration conf = new Configuration(getConf());
		log.info("=================================================================================");

		String infos_input = config.getParam("infos_input",
				"/user/recsys/pub_data/infomation");
		String filler_output = config.getParam("filler_output",
				"/user/recsys/job0007/filler_output");
		String fillerTime = config.getParam("fillerTime", "10");//过滤时间多少天内数据
		String fillerTopN = config.getParam("fillerTopN", "100");//在多少条数据中筛选
		String resultNum = config.getParam("resultNum", "100");//结果数量
		String eachResultNum = config.getParam("eachResultNum", "10");//每个结果包含几条数据
		conf.set("fillerTime", fillerTime);
		conf.set("fillerTopN", fillerTopN);
		conf.set("resultNum", resultNum);
		conf.set("eachResultNum", eachResultNum);

		// 检查输出目录
		checkOutputPath(filler_output);
		System.out.println("输入路径: " + infos_input);
		System.out.println("输出路径: " + filler_output);

		Job job = new Job(conf, "FillerInfos");

		job.setJarByClass(FillerInfosDriver.class);

		FileInputFormat.addInputPath(job, new Path(infos_input));
		FileOutputFormat.setOutputPath(job, new Path(filler_output));

		job.setMapperClass(FillerInfosMapper.class);
		job.setReducerClass(FillerInfosReducer.class);

		job.setNumReduceTasks(config.getParam("fillter_infos_reduce_task_num",
				1));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (job.waitForCompletion(true)) {
			log.info("job[" + job.getJobID()
					+ "] complete, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
		} else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): "
					+ (System.currentTimeMillis() - start));
			return 1;
		}
		return 0;
	}

	private void checkOutputPath(String fileName) {
        Logger log = PluginUtil.getInstance().getLogger();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path path = new Path(fileName);
            boolean isExists = fs.exists(path);
            if (isExists) {
                boolean isDel = fs.delete(path, true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}

package com.eb.bi.rs.frame2.recframe.base;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;

// 继承ArgsBaseMain，且使用组合模式配置文件的的应用程序，可通过此主函数来执行hadoop任务
public class ArgsBaseMain {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// -Dconfig_file=filename
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Logger log = Logger.getLogger("BaseArgsMain");
		
		if (conf.get("config_file") == null) {
			log.fatal("The Param last : -Dconfig_file=filename ");
			throw new InterruptedException();
		}
		
		PluginConfig pluginConfig = new PluginConfig();
		if (!pluginConfig.load(conf.get("config_file"), 0)) {
			log.fatal("Load config file failed.[Reason: " + pluginConfig.getErrorDesc() + "]");
			throw new InterruptedException();
		}
		log.info("Config File : " + pluginConfig.toString());

		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Date begin = new Date();
		int ret = root.run(args);
		Date end = new Date();
		long timeCost = end.getTime() - begin.getTime();

		log.info("time cost in total(s): " + (timeCost / 1000.0));
		System.exit(ret);
	}
}

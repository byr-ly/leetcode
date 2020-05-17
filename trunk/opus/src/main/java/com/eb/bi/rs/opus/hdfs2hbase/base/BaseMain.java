package com.eb.bi.rs.opus.hdfs2hbase.base;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;

// 继承BaseDriver，且使用组合模式配置文件的的应用程序，可通过此主函数来执行hadoop任务
public class BaseMain {
	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();
		PluginConfig pluginConfig = pluginUtil.getConfig();
		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Date begin = new Date();
		int ret = root.run(args);
		Date end = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(end);
		long timeCost = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC: PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully": "run failed.");
		result.save();
		log.info("time cost in total(s): " + (timeCost / 1000.0));
		System.exit(ret);
	}
}
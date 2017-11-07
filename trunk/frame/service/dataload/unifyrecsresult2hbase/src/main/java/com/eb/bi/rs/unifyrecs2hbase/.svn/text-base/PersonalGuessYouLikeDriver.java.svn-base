package com.eb.bi.rs.unifyrecs2hbase;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.eb.bi.rs.utils.ComponentHelper;
import com.eb.bi.rs.utils.JobComponent;
import com.eb.bi.rs.utils.PluginConfig;
import com.eb.bi.rs.utils.PluginExitCode;
import com.eb.bi.rs.utils.PluginResult;
import com.eb.bi.rs.utils.PluginUtil;

public class PersonalGuessYouLikeDriver {

	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();

		PluginConfig pluginConfig = pluginUtil.getConfig();
		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Date begin = new Date();

		int ret = root.run(null);

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
}

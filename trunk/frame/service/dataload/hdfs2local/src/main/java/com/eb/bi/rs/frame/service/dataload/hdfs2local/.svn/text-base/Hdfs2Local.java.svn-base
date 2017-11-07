package com.eb.bi.rs.frame.service.dataload.hdfs2local;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame.common.hadoop.fileopt.GetMergeFromHdfs;
import com.eb.bi.rs.frame.common.hadoop.fileopt.GetFromHdfs;

import org.apache.log4j.Logger;

public class Hdfs2Local {

	public static void main(String[] args) {

		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();
		Logger log = plugin.getLogger();
		long startTime = System.currentTimeMillis();
		long endTime   = 0;

		String localPaths = config.getParam("local_path", null);
		String hdfsName = config.getParam("hdfs_name", null);
		String hdfsPaths = config.getParam("hdfs_path", null);
		boolean isMerge = "true".equalsIgnoreCase(config.getParam("is_merge", null));

		if (localPaths == null || hdfsPaths == null) {
			log.error("Config error, local_path or hdfs_path is null, please check it");
			System.exit(-1);
		}
		if (hdfsName.isEmpty()) {
			hdfsName = null;
		}

		String[] hPaths = hdfsPaths.split(",");
		String[] lPaths = localPaths.split(",");
		if (hPaths.length != lPaths.length) {
			log.error("Config error, number of paths in local_path and hdfs_path not equal.");
			System.exit(-1);
		}
		for (int i = 0; i < lPaths.length; i++) {
			try {
				if (isMerge) {
					if (hdfsName == null) {
						GetMergeFromHdfs.get(hPaths[i], lPaths[i]);
					} else {
						GetMergeFromHdfs.get(hdfsName, hPaths[i], lPaths[i]);
					}
				} else {
					if (hdfsName == null) {
						GetFromHdfs.get(hPaths[i], lPaths[i]);
					} else {
						GetFromHdfs.get(hdfsName, hPaths[i], lPaths[i]);
					}
				}
			} catch (IOException e) {
				// e.printStackTrace();
				log.error(e);
			}
		}
		
		endTime = System.currentTimeMillis();
		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String strEndTime = format.format(dateEnd);		

		PluginResult result = plugin.getResult();		
		result.setParam("endTime", strEndTime);
		result.setParam("timeCosts", (endTime - startTime)/1000);
		result.setParam("exitCode", PluginExitCode.PE_SUCC);
		result.setParam("exitDesc", "run successfully");
		result.save();
		System.exit(0);
	}
}

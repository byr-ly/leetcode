package com.eb.bi.rs.frame.service.dataload.local2hdfs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame.common.hadoop.fileopt.PutToHdfs;

import org.apache.log4j.Logger;

public class Local2Hdfs {

	public static void main(String[] args) {

		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();
		Logger log = plugin.getLogger();
		long startTime = System.currentTimeMillis();
		long endTime      = 0;
		
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

		String[] lPaths = localPaths.split(",");
		String[] hPaths = hdfsPaths.split(",");
		if (lPaths.length != hPaths.length) {
			log.error("Config error, number of paths in local_path and hdfs_path not equal.");
			System.exit(-1);
		}
		for (int i = 0; i < lPaths.length; i++) {
			try {
				if (isMerge) {
					if (hdfsName == null) {
						PutMergeToHdfs.put(lPaths[i], hPaths[i]);
					} else {
						PutMergeToHdfs.put(lPaths[i], hdfsName, hPaths[i]);
					}
				} else {
					if (hdfsName == null) {
						PutToHdfs.put(lPaths[i], hPaths[i]);
					} else {
						PutToHdfs.put(lPaths[i], hdfsName, hPaths[i]);
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


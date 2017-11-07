package com.eb.bi.rs.frame.service.dataload.file2redis;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class File2Redis {
	
	public static void main(String[] args) throws Exception {


		// 配置和日志初始化
		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();
		
	
		LoaderBase loader = LoaderFactory.getLoader(config.getParam("type", "String"));	
		loader.load();
	}
}

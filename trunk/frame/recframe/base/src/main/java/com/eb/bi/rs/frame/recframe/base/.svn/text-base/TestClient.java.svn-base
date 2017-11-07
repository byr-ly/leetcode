package com.eb.bi.rs.frame.recframe.base;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class TestClient {

	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		PluginConfig pluginConfig = pluginUtil.getConfig();

		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Logger.getRootLogger().setLevel(Level.DEBUG);

		root.run(null);
	}
}

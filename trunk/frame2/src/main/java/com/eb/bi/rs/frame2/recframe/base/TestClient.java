package com.eb.bi.rs.frame2.recframe.base;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

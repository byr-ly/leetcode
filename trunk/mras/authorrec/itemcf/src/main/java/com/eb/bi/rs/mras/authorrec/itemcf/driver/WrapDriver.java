package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.LogUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;

public class WrapDriver {

	protected PluginUtil pluginUtil = PluginUtil.getInstance();
	protected LogUtil logUtil = LogUtil.getInstance();
	protected JobExecuUtil execuUtil = new JobExecuUtil();
	protected Configuration initConf = null;
	protected int reduceNo = -1;
	protected int reduceNoBig = -1;
	
	public WrapDriver(Configuration cf)
	{
		this.initConf = cf;
		this.reduceNo = pluginUtil.getReduceNum();
		this.reduceNoBig = pluginUtil.getReduceNumBig();
	}
	
	public Configuration getConf()
	{
		return this.initConf;
	}
	
}

package com.eb.bi.rs.frame.service.dataload.file2redis;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;


public abstract class LoaderBase {

	protected final static Logger log = PluginUtil.getInstance().getLogger();
	protected final static PluginConfig config = PluginUtil.getInstance().getConfig();
	protected final static PluginResult result = PluginUtil.getInstance().getResult();
	
	protected File[] files;
	protected String key;
	protected String value;
	protected LongBean cnt;
	protected File[] tmpFiles;
	protected String redisIp;
	protected int redisPort;
	
	
	protected String clearKey;
	protected boolean multiGrp;
	
	protected boolean checkBeforeClear;
	protected double threshold;

	
	protected LoaderBase() {		
		String fileName = config.getParam("file", null);		
		File file = new File(fileName);
		if (!file.exists()) {
			log.error(fileName + " does not exist");
			System.exit(-1);
		}
		if (file.isDirectory()) {			
			FileFilter filter = new FileFilter() {				
				public boolean accept(File pathname) {
					return !pathname.getName().endsWith(".crc") && !pathname.getName().startsWith("_");
				}
			};	
			files = file.listFiles(filter);			
		} else {
			files = new File[]{file};
		}
		this.tmpFiles = new File[files.length];
		this.key =  config.getParam("key", null);
		this.value = config.getParam("value", null);		
		this.redisIp = config.getParam("redisIp", null);
		this.redisPort = config.getParam("redisPort", 0);
		
		this.clearKey = config.getParam("clearKey", null);		
		this.multiGrp = config.getParam("lineContainsMultiGroup", false);
				
		this.checkBeforeClear = config.getParam("checkBeforeClear", false);
		this.threshold = 1.0;
		if (checkBeforeClear) {
			threshold = config.getParam("threshold", 1.0);
		}	
		cnt = new LongBean(0);		
	}
	
	
	public boolean clearRedis()  {
		
		if (clearKey != null) {
			Jedis jedis = null;
			try {				
				jedis = new Jedis(redisIp, redisPort);
				Set<String> keys = jedis.keys(clearKey);
				if (checkBeforeClear) {
					if (Math.abs(cnt.getRecordcnt() - keys.size() )  >  threshold * keys.size()) {
						log.error("check failed. input key count[" + cnt.getRecordcnt() + "] is far away from  prior key count[" + keys.size() + "]");
						for (File tmpFile : tmpFiles) {
							tmpFile.delete();								
						}							
						return false;
					} else {
						log.info("check success. input key count[" + cnt.getRecordcnt() + "] is close to prior key count[" + keys.size() + "]");
					}
				}
				
//				Pipeline pipelined = jedis.pipelined();
//				for (String existkey : keys) {
//					pipelined.del(existkey);
//				}
//				pipelined.sync();
//				log.info("delete records in redis success.");				
			} finally {
				if (jedis != null) {
					jedis.close();				
				}
			}
			
		}
		return true;	
	}
	
	public boolean load() throws IOException {
		Long startTime = System.currentTimeMillis();
		if (!prepare()) {			
			log.error("prepare data error"); 
			return false;
		}		
		long preparedTime = System.currentTimeMillis();
		log.info("prepare " + cnt.getRecordcnt() + " data consumed time(s): " + ((preparedTime - startTime) / 1000));	
		if (!clearRedis()) {		
			return false;
		}
		//test
		if (!loadCore()) {
			return false;
		}
		long endTime = System.currentTimeMillis();
		Date dateEnd = new Date();
		String strEndTime =  new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd);		
				
		result.setParam("endTime", strEndTime);
		result.setParam("timeCosts", (endTime - startTime)/1000);
		result.setParam("exitCode", PluginExitCode.PE_SUCC);
		result.setParam("exitDesc", "run successfully");
		result.save();		
		log.info("transfer data consumed time(s):" + (endTime - preparedTime) / 1000 + "  key value pairs:" +  cnt.getRecordcnt());
		return true;
	}
	
	
	
	public abstract boolean prepare() throws IOException ;
	public abstract boolean loadCore() throws IOException ;
	

}

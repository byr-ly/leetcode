package com.eb.bi.rs.frame2.service.dataload.file2redis;//package com.eb.bi.rs.frame.service.dataload.file2redis;
//
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashSet;
//import java.util.Set;
//
//import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
//import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
//import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
//import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//
//import org.apache.log4j.Logger;
//
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Pipeline;
//
///**
// * @author zzl
// */
//
//public class CopyOfFile2Redis {
//	
//	public static void main(String[] args)
//	{
//	
//		// 配置和日志初始化
//		PluginUtil plugin = PluginUtil.getInstance();
//		plugin.init(args);
//		PluginConfig config = plugin.getConfig();
//		Logger log = plugin.getLogger();
//		
//		// 配置参数初始化
//		String redisIp = null;
//		int redisPort = 0;
//		String fileName = null;
//		String type = null;
//		String key = null;
//		String value = null;
//		String hashField = null;
//		String clearKey = null;
//		String delCmd = null;
//		
//		// redis数据库连接信息读取
//		redisIp = config.getParam("redisIp", null);
//		redisPort = Integer.parseInt(config.getParam("redisPort", "0"));
//		// 导入相关配置信息读取
//		fileName = config.getParam("file", null);
//		type = config.getParam("type", "String");
//		key = config.getParam("key", null);
//		value = config.getParam("value", null);
//		hashField = config.getParam("field", null);
//		clearKey = config.getParam("clearKey", null);
//		
//		//
//		boolean multiVal = config.getParam("multiValues", false);
//		int stepLen = 1;                    
//		if (multiVal) {
//			stepLen = config.getParam("stepLength", 1);
//		}
//		
//		String command = config.getParam("command", "rpush");
//		boolean checkBeforeClear = config.getParam("checkBeforeClear", false);
//		double  threshold = 1.0;
//		if (checkBeforeClear) {
//			threshold = config.getParam("threshold", 1.0);
//		}
//		
//
//		if ( clearKey != null)
//		{
//			delCmd = "redis-cli -h " + redisIp + " -p " +  redisPort + " KEYS \"" + clearKey + "\" |xargs redis-cli -h " + redisIp + " -p " + redisPort + " del";
//		}
//		
//		if ((redisIp == null) || (redisPort == 0) || (fileName == null) || (key == null) || (value == null) || (type == null))
//		{
//			log.error("config file error, please check config items to ensure each item correctly");
//			System.exit(-1);
//		}
//		
//		long startTime    = 0;
//		long preparedTime = 0;
//		long endTime      = 0;
//		LongBean cnt = new LongBean(0);
//		
//		startTime = System.currentTimeMillis();
//		
//		if (type.equalsIgnoreCase("Set"))
//		{
//			cnt.setRecordcnt(0);
//			if (!SetLoader.prepareData(fileName, key, value, cnt))
//			{
//				log.error("prepare data error : " + fileName + " lines : " + cnt.getRecordcnt()); 
//				System.exit(-1);
//			}
//			
//			preparedTime = System.currentTimeMillis();
//			log.info("prepare " + cnt.getRecordcnt() + " data consumed time(s): " + ((preparedTime - startTime) / 1000));
//			
//			if (clearKey != null)
//			{
//				log.info("delCmd is " + delCmd);
//				
//				Runtime run = Runtime.getRuntime(); 
//			    Process process = null;          
//			    try 
//		        {  
//			    	process = run.exec(delCmd); //
//		            process.waitFor();  
//		            log.info("delete records in redis success.");
//		        }
//			    catch (Exception e) 
//		        {          	
//		        	e.printStackTrace();
//		            log.error("delete record in redis error.");  
//		        }  			
//			}
//			
//			cnt.setRecordcnt(0);
//			boolean isOk = SetLoader.load(fileName, redisIp, redisPort, cnt);
//			
//			if (isOk)
//			{
//				log.info(fileName + " has been transformed " + cnt.getRecordcnt() + " to redis Set" );
//			}
//			else
//			{
//				log.error(fileName + " has not been transformed to redis Set");
//				System.exit(-1);
//			}
//		}
//		else if (type.equalsIgnoreCase("String"))
//		{
//			cnt.setRecordcnt(0);
//			if (!StringLoader.prepareData(fileName, key, value, cnt))
//			{
//				log.error("prepare data error : " + fileName + " lines : " + cnt.getRecordcnt()); 
//				System.exit(-1);
//			}
//			
//			preparedTime = System.currentTimeMillis();
//			log.info("prepare " + cnt.getRecordcnt() + " data consumed time(s): " + ((preparedTime - startTime) / 1000));
//			
//			if (clearKey != null)
//			{
//				log.info("delCmd is " + delCmd);
//				
//				Runtime run = Runtime.getRuntime(); 
//			    Process process = null;          
//			    try 
//		        {  
//			    	process = run.exec(delCmd); 
//		            process.waitFor();  
//		            log.info("delete records in redis success.");
//		        }
//			    catch (Exception e) 
//		        {          	
//		        	e.printStackTrace();
//		            log.error("delete record in redis error.");  
//		        }  			
//			}
//			
//			boolean isOk = StringLoader.load(fileName, redisIp, redisPort, cnt);
//			
//			if (isOk)
//			{
//				log.info(fileName + " has been transformed " + cnt.getRecordcnt() + " to redis String");
//			}
//			else
//			{
//				log.error(fileName + " has not been transformed to redis String. records : " + cnt.getRecordcnt());
//				System.exit(-1);
//			}
//		}
//		else if (type.equalsIgnoreCase("Hash") && (hashField != null))
//		{
//			cnt.setRecordcnt(0);
//			if (!HashLoader.prepareData(fileName, key, hashField, value, cnt))
//			{
//				log.error("prepare data error : " + fileName + " lines : " + cnt.getRecordcnt()); 
//				System.exit(-1);
//			}
//			
//			preparedTime = System.currentTimeMillis();
//			log.info("prepare " + cnt.getRecordcnt() + " data consumed time(s): " + ((preparedTime - startTime) / 1000));
//			
//			if (clearKey != null)
//			{
//				log.info("delCmd is " + delCmd);
//				
//				Runtime run = Runtime.getRuntime(); 
//			    Process process = null;          
//			    try 
//		        {  
//			    	process = run.exec(delCmd); 
//		            process.waitFor();  
//		            log.info("delete records in redis success.");
//		        }
//			    catch (Exception e) 
//		        {          	
//		        	e.printStackTrace();
//		            log.error("delete record in redis error.");  
//		        }  			
//			}
//			
//			boolean isOk = HashLoader.load(fileName, redisIp, redisPort, cnt);
//			
//			if (isOk)
//			{
//				log.info(fileName + " has been transformed " + cnt.getRecordcnt() + " to redis Hash");
//			}
//			else
//			{
//				log.error(fileName + " has not been transformed to redis Hash");
//				System.exit(-1);
//			}
//		}
//		else if (type.equalsIgnoreCase("List"))
//		{
//			cnt.setRecordcnt(0);
//			if (!ListLoader.prepareData(fileName, key, value, cnt, multiVal, stepLen)) {
//				log.error("prepare data error : " + fileName + " lines : " + cnt.getRecordcnt()); 
//				System.exit(-1);
//			}
//			
//			preparedTime = System.currentTimeMillis();
//			log.info("prepare " + cnt.getRecordcnt() + " data consumed time(s): " + ((preparedTime - startTime) / 1000));
//			
//			if (clearKey != null)
//			{
//				Jedis jedis = null;
//				try {
//					jedis = new Jedis(redisIp, redisPort);
//					Set<String> keys = jedis.keys(clearKey);
//					if (checkBeforeClear) {
//						if ( Math.abs(cnt.getRecordcnt() - keys.size() )  >  threshold * keys.size()) {
//							log.error("check failed. input key count[" + cnt.getRecordcnt() + "] is far away from  prior key count[" + keys.size() + "]");
//							System.exit(-1);
//						} else {
//							log.info("check success. input key count[" + cnt.getRecordcnt() + "] is close to prior key count[" + keys.size() + "]");
//						}
//					}
//					
//					Pipeline pipelined = jedis.pipelined();
//					for (String existkey : keys) {
//						pipelined.del(existkey);
//					}
//					pipelined.sync();
//					log.info("delete records in redis success."  );
//					
//				} catch (Exception e) {
//		        	e.printStackTrace();
//		            log.error("delete record in redis error. ");  
//				} finally {
//					if (jedis != null) {
//						jedis.close();
//						jedis = null;
//					}
//					
//				}			
//			}
//			/*bug:如果在这之前程序退出那么会导致中间文件没有被删除，然后被当做原始文件处理*/
//			boolean isOk = ListLoader.load(fileName, redisIp, redisPort, cnt, command);
//			if (isOk)
//			{
//				log.info(fileName + " has been transformed " + cnt.getRecordcnt() + " to redis List");
//			}
//			else
//			{
//				log.error(fileName + " has not been transformed to redis List");
//				System.exit(-1);
//			}
//		}
//		else 
//		{
//			log.error("Unknow redis data type!");
//			System.exit(-1);
//		}
//		endTime = System.currentTimeMillis();
//		Date dateEnd = new Date();
//		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
//		String strEndTime = format.format(dateEnd);		
//		
//		PluginResult result = plugin.getResult();		
//		result.setParam("endTime", strEndTime);
//		result.setParam("timeCosts", (endTime - startTime)/1000);
//		result.setParam("exitCode", PluginExitCode.PE_SUCC);
//		result.setParam("exitDesc", "run successfully");
//		result.save();
//		
//		log.info("transfer data consumed time(s):" + (endTime - preparedTime)/1000 + "  records:" +  cnt.getRecordcnt());
//		System.exit(0);
//	}
//
//	public static void usage()
//	{
//		System.out.println("usage : java -jar file2redis.jar -job_id=jobid -task_id=taskId -batch_id=batchId -plugin_name=pluginName");
//	}
//}

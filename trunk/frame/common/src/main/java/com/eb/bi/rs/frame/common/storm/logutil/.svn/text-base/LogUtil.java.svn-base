package com.eb.bi.rs.frame.common.storm.logutil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

public class LogUtil {
	
	private Logger logger = null;
	private static String logConfPath = System.getenv("STORM_HOME")+"/log4j/storm.log.properties";//TODO ??????

	private String app_logFilePath;
	
	private static class SingletonHolder {
		static final LogUtil INSTANCE = new LogUtil();
	}

	public static LogUtil getInstance() {
		return SingletonHolder.INSTANCE;
	}
	
	public LogUtil() {
	}
	
	public Logger getLogger(){
		return logger;
	}
	
	public String getLogConfPath() {
		return logConfPath;
	}
	
	public void setLogConfPath(String path) {
		logConfPath = path;
	}

	public void init(String appName, String workerName, String nodeName) {
		/*
		String storm_home = System.getenv("STORM_HOME");
		System.setProperty("app_storm.home", storm_home);
		
		String app_path = appName + "/";
		System.setProperty("app.path", app_path);
		
		String log_name = appName + ".log";
		System.setProperty("log.name", log_name);
		*/
		
		logger = Logger.getLogger("recom_apps");
		Properties props = new Properties();
		try {
			FileInputStream istream = new FileInputStream(logConfPath);
			props.load(istream);
			istream.close();
			/*props.setProperty("log4j.appender.appender1.File", 
					System.getenv("STORM_HOME")+"/logs/app/"+appName+"_"+
					workerName+"_"+nodeName+".log");*/

			/*
			props.setProperty("log4j.appender.appender1.File", 
					System.getenv("STORM_HOME")+"/logs/app/"+appName+"_"+
					workerName+".log");
			*/
			
			PropertyConfigurator.configure(props);// ???log4j???????
			System.out.println("path = "+ props.getProperty("recom_apps"));
		} catch (IOException e) {
			System.out.println("Could not read configuration file ["
					+ logConfPath + "].");
		}
		
		PropertyConfigurator.configure(props);
	}

}


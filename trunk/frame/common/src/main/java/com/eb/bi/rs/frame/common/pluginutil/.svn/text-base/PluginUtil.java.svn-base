
package com.eb.bi.rs.frame.common.pluginutil;
 
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

// pluginExe -job_id -task_id -batch_id -plugin_name -config_type -rep_path -log_category -log_config_path
public class PluginUtil
{
	private static PluginUtil instance = null;
	private static final String idoxConfigFileName = "config.idox";
	private static final String defLogConfigFileName = "log4j.properties";
	private static final String defLogCategory = "idoxplugin";
	
	private String rootPath;
	private String repPath;
	private String pluginName;
	private PluginParam pluginParam;
	private PluginConfig pluginConfig;
	private PluginResult pluginResult;
	private Logger pluginLogger;

	private String jobId;
	private String taskId;
	private String batchId;
	private String baseTime;
	private String beginTime;

	private String metaDataPath;
	private String runBatchPath;
	private String configFilePath;
	private String resultFilePath;
	private String logFilePath;
	private String logConfigFilePath;
	private String logCategory;

	private boolean isInit = false;

	public static PluginUtil getInstance()
	{
		if (instance == null)
		{
			instance = new PluginUtil();
		}
		return instance;
	}

	private PluginUtil()
	{
	}

	private String usage()
	{
		return new String(
				"pluginName\n\t<-job_id=${Job.Id}>\n\t<-task_id=${Task.Id}>\n\t<-batch_id={Batch.Id}>\n\t[-plugin_name=jarfilename]\n\t[-baseTime=YYYYMMDDhhmmss]\n\t[-config_type=0|1]\n\t[-rep_path=${IDOX.Repository.Path}]\n\t[-log_category=${Log.Category}|default]\n\t[-log_config_path=${Log.ConfigPath}]");
	}

	public boolean init(String[] args)
	{
		boolean ret = true;
		ret = ret && initParam(args);
		ret = ret && initResource();
		ret = ret && initLog();
		ret = ret && initConfig();
		ret = ret && initResult();

		if (!ret)
		{
			System.out.println(usage());
		}
		isInit = ret;
		return ret;
	}

	private boolean initParam(String[] args)
	{
		// Step 0, Get current time and root path of IDOX.
		java.util.Calendar c = java.util.Calendar.getInstance();
		java.text.SimpleDateFormat f = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
		beginTime = f.format(c.getTime());

		rootPath = System.getenv("IDOXDIR");
		
		System.out.println(rootPath + "is it emtpy?");
		
		if (rootPath == null || !new File(rootPath).canWrite())
		{
			System.out.println("ERROR: Can not find $IDOXDIR or $IDOXDIR can not write");
			return false;
		}

		

		// Step2, Get and set plugin params
		pluginParam = new PluginParam();
		pluginParam.addCheckedParam("-job_id");
		pluginParam.addCheckedParam("-task_id");
		pluginParam.addCheckedParam("-batch_id");

		//pluginParam.setParam("-plugin_name", pluginName);
		//��args�������ø�pluginParam
		if (!pluginParam.parse(args))
		{
			System.out.println(pluginParam.getErrorDesc());
			System.out.println(usage());
			return false;
		}

		jobId = pluginParam.getParam("-job_id");
		taskId = pluginParam.getParam("-task_id");
		batchId = pluginParam.getParam("-batch_id");
		
		
		/*for hadoop jar xxx.jar*/
		if(pluginParam.paramExists("-plugin_name")){
			pluginName = pluginParam.getParam("-plugin_name");
		}
		else {/*for java xxx.jar*/
			/* Step 1, Get plugin name,
			 * URL under Linux like this:
			 * file:/home/zhanglei/java_workspace/remote.jar!/com/ebupt/common/
			 * 
			 */
			URL url = this.getClass().getResource("");
			System.out.println(String.format("URL: path=%s,  file=%s", url.getPath(), url.getFile()));
			String[] s = url.getPath().split("/");
			for (String l : s)
			{
				if (l.contains(".jar!"))
				{
					pluginName = l.replace(".jar!", "");
					break;
				}
			}
		}
		
		if (pluginParam.paramExists("-base_time"))
		{
			baseTime = pluginParam.getParam("-base_time");
		} else
		{
			baseTime = beginTime;
			pluginParam.setParam(new String("-base_time"), baseTime);
		}

		if (pluginParam.paramExists("-rep_path"))  //rep_path������
		{
			repPath = pluginParam.getParam("-rep_path");
		} else
		{
			repPath = rootPath + "/repository";
			// Decide real repository path from config.idox
			File configFile = new File(rootPath + "/etc/" + this.idoxConfigFileName);
			SAXReader reader = new SAXReader();
			Document doc = null;
			try
			{
				doc = reader.read(configFile);
				Element element = doc.getRootElement().element("repository").element("savePath");
				if (element != null && !element.attributeValue("value").trim().isEmpty())
				{
					repPath = element.attributeValue("value").trim();
				}
			} catch (DocumentException e)
			{
				System.out.println(String.format("ERROR: Read idox config file failed.[File: %s, Reason: %s]", configFile.toString(), e.getMessage()));
				return false;
			}
		}
		
		if (!new File(repPath).canWrite())
		{
			System.out.println("ERROR: IDOX repository path can not write. Path = " + repPath);
			return false;
		}
		pluginParam.setParam("-rep_path", repPath);

		// Step 3 , initialize common avariables
		Assert.assertTrue(pluginParam.isValid() && !pluginName.isEmpty());
		metaDataPath = repPath + "/" + jobId + "/meta_data";
		runBatchPath = repPath + "/" + jobId + "/running_batch/" + batchId;
		


		configFilePath = String.format("%s/%s.task%s.config", metaDataPath, pluginName, taskId);		
		resultFilePath = String.format("%s/%s.task%s.result", runBatchPath, pluginName, taskId);
		logFilePath = String.format("%s/%s.task%s.log", runBatchPath, pluginName, taskId);
		logCategory = defLogCategory;
		if (pluginParam.paramExists("-log_category"))
		{
			logCategory = pluginParam.getParam("-log_category");
		}

		logConfigFilePath = rootPath + "/etc/" + defLogConfigFileName;
		if (pluginParam.paramExists("-log_config_path"))
		{
			logConfigFilePath = pluginParam.getParam("-log_config_path");
		}

		return true;
	}

	private boolean initResource()
	{
		// TODO: I don't know how to set RLIMIT_NOFILE,RLIMIT_CORE,RLIMIT_NOFILE
		// as c++ coding
		return true;
	}

	private boolean initLog()
	{
		System.setProperty("log.file.path", logFilePath);
		PropertyConfigurator.configure(logConfigFilePath);
		pluginLogger = Logger.getLogger(logCategory);
		if (pluginLogger == null)
		{
			System.out.println("ERROR: Initialize log failed.");
			return false;
		}

		// Print test
		pluginLogger.info("============================================");
		pluginLogger.info("pluginName=" + pluginName);
		pluginLogger.info("pluginParam=" + pluginParam.toString());
		pluginLogger.info("rootPath=" + rootPath);
		pluginLogger.info("repPath=" + repPath);
		pluginLogger.info("metaDataPath=" + metaDataPath);
		pluginLogger.info("runBatchPath=" + runBatchPath);
		pluginLogger.info("configFilePath=" + configFilePath);
		pluginLogger.info("resultFilePath=" + resultFilePath);
		pluginLogger.info("logFilePath=" + logFilePath);
		pluginLogger.info("logCategory=" + logCategory);
		pluginLogger.info("logConfigFilePath=" + logConfigFilePath);
		pluginLogger.info("============================================");
		return true;
	}

	private boolean initConfig()
	{
		pluginConfig = new PluginConfig();
		int configType = 0;
		if(pluginParam.paramExists("-config_type")){
			configType = Integer.parseInt(pluginParam.getParam("-config_type"));
		}
		
		if (!pluginConfig.load(configFilePath, configType))
		{
			pluginLogger.fatal("ERROR: Load config file failed.[Reason: " + pluginConfig.getErrorDesc() + "]");
			return false;
		}

		pluginLogger.debug("pluginConfig=" + pluginConfig.toString());
		return true;
	}

	private boolean initResult()
	{
		pluginResult = new PluginResult(resultFilePath);
		pluginResult.setParam("beginTime", beginTime);
		pluginResult.setParam("jobId", jobId);
		pluginResult.setParam("batchId", batchId);
		pluginResult.setParam("taskId", taskId);
		pluginResult.setParam("log.file.path", logFilePath);
		pluginResult.setParam("result.file.path", resultFilePath);

		pluginLogger.debug("pluginResult=" + pluginResult.toString());
		return true;
	}

	public PluginParam getParam()
	{
		Assert.assertTrue(isInit);
		return pluginParam;
	}

	public PluginConfig getConfig()
	{
		Assert.assertTrue(isInit);
		return pluginConfig;
	}

	public PluginResult getResult()
	{
		Assert.assertTrue(isInit);
		return pluginResult;
	}

	public Logger getLogger()
	{
		Assert.assertTrue(isInit);
		return pluginLogger;
	}

//	public static void main(String[] args)
//	{
//		PluginUtil plugin = PluginUtil.getInstance();
//		plugin.init(args);
//
//		System.out.println(plugin.getParam().toString());
//		System.out.println(plugin.getConfig().toString());
//		System.out.println(plugin.getResult().toString());
//		plugin.getLogger().info("OK");
//	}
}

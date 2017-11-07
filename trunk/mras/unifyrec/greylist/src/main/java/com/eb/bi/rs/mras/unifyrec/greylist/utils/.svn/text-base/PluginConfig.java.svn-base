package com.eb.bi.rs.mras.unifyrec.greylist.utils;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * @author zhanglei
 * @date 2012-10-23
 * 
 */
public class PluginConfig
{
	// 0 stand for name-value format, 1 stand for other format.
	// Don't parse config file to params when configType is not 0;
	private int configType;
	
	private String errorDesc;
	private String configFilePath;
	private Properties params = new Properties();

	private Element root;

	public boolean load(String _configFilePath, int _configType)
	{
		this.configFilePath = _configFilePath;
		this.configType = _configType;
		File file = new File(configFilePath);
		if (!file.exists() || !file.canRead())
		{
			errorDesc = String.format("Plugin config file (%s) doesn't exist or can not read", configFilePath);
			return false;
		}
		
		// 当配置文件类型为不为0（name-value形式），则不解析配置文件，由应用解析。
		if (_configType != 0)
		{
			return true;
		}
		
		// Read content from config file
		SAXReader reader = new SAXReader();
		Document doc = null;
		try
		{
			doc = reader.read(configFilePath);
			root = doc.getRootElement();
			List nodes = doc.getRootElement().elements("param");
			for (Iterator it = nodes.iterator(); it.hasNext();)
			{
				Element element = (Element) it.next();
				String name = element.attributeValue("name");
				String value = element.getText();
				if (name == null || name.trim().isEmpty())
				{
					continue;
				}
				params.put(name, value);
			}
		} catch (DocumentException e)
		{
			errorDesc = String.format("Read config file failed.[File:%s, Reason: %s]", configFilePath, e.getMessage());
			return false;
		}

		return true;
	}

	public boolean getParam(String name, final boolean defValue)
	{
		return isParamExists(name) ? params.getProperty(name).equalsIgnoreCase("true") : defValue;
	}

	public String getParam(String name, final String defValue)
	{
		return params.getProperty(name, defValue);
	}

	public int getParam(String name, final int defValue)
	{
		return isParamExists(name) ? Integer.parseInt(params.getProperty(name)) : defValue;
	}
	
	
	public double getParam(String name, final double defValue)
	{
		return isParamExists(name) ? Double.parseDouble(params.getProperty(name)) : defValue;
	}
	
	public float getParam(String name, final float defValue)
	{
		return isParamExists(name) ? Float.parseFloat(params.getProperty(name)) : defValue;
	}
	
	

	public boolean isParamExists(String name)
	{
		return params.containsKey(name);
	}

	public Properties getParams()
	{
		return params;
	}

	public int getConfigType()
	{
		return configType;
	}

	public String getErrorDesc()
	{
		return errorDesc;
	}

	public String getConfigFilePath()
	{
		return configFilePath;
	}

	public Element getRootElement() {
		return root;
	}

	public Element getElement(String name) {
		return root.element(name);
	}

	public String toString()
	{
		return "Config File:" + configFilePath + " Config Type:" + Integer.toString(configType) + " Config List:" + params.toString();
	}
}

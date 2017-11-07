package com.eb.bi.rs.frame.common.pluginutil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
public class PluginResult
{
	private String errorDesc;
	private String resultFilePath;
	private Properties results = new Properties();

	public PluginResult()
	{
	}

	public PluginResult(String _resultFilePath)
	{
		this.resultFilePath = _resultFilePath;
	}

	public String getResultFilePath()
	{
		return resultFilePath;
	}

	void setResultFilePath(String _resultFilePath)
	{
		this.resultFilePath = _resultFilePath;
	}

	public boolean load()
	{
		return load(resultFilePath);
	}

	public boolean load(String _resultFilePath)
	{
		File file = new File(_resultFilePath);
		if (!file.exists() || !file.canRead())
		{
			errorDesc = String.format("Read result file failed. [File: %s, Reason: result file doesn't exist or can not read.]", resultFilePath);
			return false;
		}

		// / Read content from config file
		SAXReader reader = new SAXReader();
		Document doc = null;
		try
		{
			doc = reader.read(_resultFilePath);
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
				results.put(name, value);
			}
		} catch (DocumentException e)
		{
			errorDesc = String.format("Read result file failed.[File:%s, Reason: %s]", resultFilePath, e.getMessage());
			return false;
		}

		return true;
	}

	public boolean save()
	{
		return save(resultFilePath);
	}

	public boolean save(String _resultFilePath)
	{
		try
		{
			FileWriter fw = new FileWriter(_resultFilePath);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("<?xml version=\"1.0\" encoding=\"GB2312\"?>\n");
			bw.write("<result>\n");
			Iterator<Map.Entry<Object, Object>> it = results.entrySet().iterator();
			while (it.hasNext())
			{
				Entry<Object, Object> entry = it.next();
				bw.write("<param name=\"" + entry.getKey() + "\">" + entry.getValue() + "</param>\n");
			}
			bw.write("</result>\n");
			bw.flush();
			fw.flush();
			bw.close();
			fw.close();
		} catch (IOException e)
		{
			e.printStackTrace();
			errorDesc = String.format("Save result file failed.[File:%s, Reason: %s]", _resultFilePath, e.getMessage());
			return false;
		}

		return true;
	}

	public void setParam(String name, boolean value)
	{
		results.setProperty(name, value == true ? "True" : "False");
	}
	
	public boolean getParam(String name, final boolean defValue)
	{
		return isParamExists(name) ? results.getProperty(name).equalsIgnoreCase("true") : defValue;
	}

	public void setParam(String name, String value)
	{
		results.setProperty(name, value);
	}

	public String getParam(String name, final String defValue)
	{
		return results.getProperty(name, defValue);
	}

	public void setParam(String name, int value)
	{
		results.setProperty(name, Integer.toString(value));
	}
	public void setParam(String name, Long value)
	{
		results.setProperty(name, Long.toString(value));
	}

	public int getParam(String name, final int defValue)
	{
		return isParamExists(name) ? Integer.parseInt(results.getProperty(name)) : defValue;
	}

	boolean isParamExists(String name)
	{
		return results.containsKey(name);
	}

	public Properties getResults()
	{
		return results;
	}

	public String getErrorDesc()
	{
		return errorDesc;
	}

	public String toString()
	{
		return "Result List:" + results.toString();
	}
}

package com.eb.bi.rs.mras.unifyrec.greylist.utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

public class PluginParam
{

	// The parsed input params, list of key-value
	private Properties m_params = new Properties();
	// The literal string of the input params
	private String m_literalParam = "";
	// Used to indicate the parameters are resolved
	private boolean m_isParsed;
	// Used to indicate the parameters are correctly resolved
	private boolean m_isValid;

	private String m_errorDesc;

	private List<String> m_checkedParams = new ArrayList<String>();

	public boolean parse(String[] args)
	{
		// Step 1, Parse and check input params
		if (!parseParams(args))
		{
			return false;
		}

		m_isParsed = true;

		// Step 2, Check if required params exists. Premise: these parameters
		// have been added to m_checkedParams
		if (!checkParams())
		{
			return false;
		}
		m_isValid = true;
		return m_isValid;
	}

	private boolean parseParams(String[] args)
	{
		for (String arg : args)
		{
			int pos = arg.indexOf('=');
			if (pos > -1)
			{
				m_params.put(arg.substring(0, pos).trim(), arg.substring(pos + 1).trim());
			} else
			{
				m_params.put(arg.trim(), "");
			}

			// m_literalParam = m_literalParam + arg + " ";
		}
		return true;
	}

	private boolean checkParams()
	{
		for (int i = 0; i < m_checkedParams.size(); i++)
		{
			String paramName = m_checkedParams.get(i);
			if (!paramExists(paramName))
			{
				m_errorDesc = "Required parameter(" + paramName + ") doesn't exist";
				return false;
			} else if (getParam(paramName).isEmpty())
			{
				m_errorDesc = "Required parameter(" + paramName + ") has empty value";
				return false;
			}
		}
		return true;
	}

	public boolean paramExists(String paramName)
	{
		return this.m_params.containsKey(paramName);
	}

	public Object setParam(Object paramName, Object paramValue)
	{
		return this.m_params.put(paramName, paramValue);
	}

	public String getParam(String paramName)
	{
		// Assert.assertTrue(m_isParsed & m_isValid);
		return this.m_params.getProperty(paramName);
	}

	public void addCheckedParam(String paramName)
	{
		m_checkedParams.add(paramName);
	}

	public boolean isValid()
	{
		return m_isValid;
	}

	public String getErrorDesc()
	{
		return m_errorDesc;
	}

	public Properties getParams()
	{
		return m_params;
	}

	public String toString()
	{
		return "Param List:" + m_params.toString();
	}
}

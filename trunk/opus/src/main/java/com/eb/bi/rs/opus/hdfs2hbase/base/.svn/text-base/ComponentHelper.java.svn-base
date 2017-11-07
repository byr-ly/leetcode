package com.eb.bi.rs.opus.hdfs2hbase.base;

import org.apache.log4j.Logger;
import org.dom4j.Element;

import java.util.Iterator;
import java.util.Properties;

/**
 * 根据XML配置构造组件
 */
public class ComponentHelper {

    private static final Logger LOG = Logger.getLogger(ComponentHelper.class);

    public static JobComposite createComposite(Element root) {
        if (!"composite".equals(root.getName())) {
            LOG.error("composite element expected, but " + root.getName() + " element found.");
            return null;
        }

        String name = root.attributeValue("name");
        JobComposite composite = new JobComposite(name);

        for (Iterator<Element> iter = root.elementIterator(); iter.hasNext(); ) {
            Element ele = iter.next();
            if ("composite".equals(ele.getName())) {
                JobComposite child = createComposite(ele);
                if (child != null) {
                    composite.add(child);
                }
            } else if ("leaf".equals(ele.getName())) {
                ToolLeaf child = createLeaf(ele);
                if (child != null) {
                    composite.add(child);
                }
            } else {
                LOG.error("composite or leaf expected, but " + ele.getName() + " found.");
            }
        }

        return composite;
    }

    public static ToolLeaf createLeaf(Element root) {
        if (!"leaf".equals(root.getName())) {
            LOG.error("leaf element expected, but " + root.getName() + " element found.");
            return null;
        }

        String name = root.attributeValue("name");
        String className = root.element("class").getText();
        BaseDriver tool;
        try {
            tool = Class.forName(className).asSubclass(BaseDriver.class).newInstance();
        } catch (Exception e) {
            LOG.error("class " + className + " initialize failed.");
            return null;
        }

        Element conf = root.element("configuration");
        if (conf == null) {
            return new ToolLeaf(name, tool);
        }

        Properties prop = new Properties();
        for (Iterator<Element> iter = conf.elementIterator(); iter.hasNext(); ) {
            Element ele = iter.next();
            String eName = ele.getName();
            String eValue = ele.getText();
            prop.put(eName, eValue);
        }

        tool.setProperties(prop);
        return new ToolLeaf(name, tool);
    }
}
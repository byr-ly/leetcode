package com.eb.bi.rs.frame2.common.storm.config;

import backtype.storm.Config;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Iterator;


/**
 * 用于读取并解析xml格式的配置文件
 *
 * @author szj
 * @since JDK1.6
 */
public class ConfigReader {
    //private PluginConfig pluginConfig = new PluginConfig();
    private String configPath;

    private static class SingletonHolder {
        static final ConfigReader INSTANCE = new ConfigReader();
    }

    public static ConfigReader getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private ConfigReader() {
    }

    public void init(String appName, Config conf) {
        String baseDir = System.getenv("HOME");
        configPath = baseDir == null ? appName : baseDir + "/cx_recommend/config/" + appName + "/" + appName + ".config";
        readConfig(configPath, conf);
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public void reloadConfig(Config conf) {
        readConfig(configPath, conf);
    }

    @SuppressWarnings({"rawtypes"})
    public void readConfig(String confPath, Config conf) {
        File xmlFile = new File(confPath);
        PluginConfig pluginConfig = new PluginConfig();

        if (xmlFile.exists()) {
            SAXReader reader = new SAXReader();
            try {
                Document document = reader.read(xmlFile);
                Element root = document.getRootElement();
                //common params
                for (Iterator iterator = root.elementIterator("param"); iterator
                        .hasNext(); ) {
                    Element param = (Element) iterator.next();

                    pluginConfig.putParam(param.attributeValue("name").trim(),
                            param.getText());
                }
                //input datas
                for (Iterator iterator = root.elementIterator("input_data"); iterator
                        .hasNext(); ) {
                    Element data = (Element) iterator.next();

                    ConfData confData = new ConfData(
                            data.attributeValue("type").trim(),
                            data.attributeValue("link").trim(),
                            data.attributeValue("loadCommand").trim(),
                            data.attributeValue("meta").trim());
                    pluginConfig.putInConfData(
                            data.attributeValue("name").trim(), confData);
                }
                //output datas
                for (Iterator iterator = root.elementIterator("output_data"); iterator
                        .hasNext(); ) {
                    Element data = (Element) iterator.next();

                    ConfData confData = new ConfData(
                            data.attributeValue("type").trim(),
                            data.attributeValue("link").trim(),
                            data.attributeValue("loadCommand").trim(),
                            data.attributeValue("meta").trim());
                    pluginConfig.putOutConfData(
                            data.attributeValue("name").trim(), confData);
                }
                //records
                for (Iterator iterator = root.elementIterator("record"); iterator
                        .hasNext(); ) {
                    Element record = (Element) iterator.next();
                    ConfRecord confRecord = new ConfRecord(
                            record.attributeValue("parseType"),
                            record.attributeValue("fieldDelimiter"),
                            record.attributeValue("recordDelimiter"));
                    int i = 0;
                    for (Iterator iti = record.elementIterator("field"); iti
                            .hasNext(); i++) {
                        Element field = (Element) iti.next();
                        ConfRecordField confRecordField = new ConfRecordField(i,
                                Boolean.parseBoolean(field.attributeValue("nullable")),
                                Integer.parseInt(field.attributeValue("size")),
                                field.attributeValue("type"));
                        confRecord.putField(
                                field.attributeValue("name").trim(),
                                confRecordField);
                    }
                    pluginConfig.putConfRecord(record.attributeValue("name"),
                            confRecord);

                    conf.put("AppConfig", pluginConfig);
                }

            } catch (DocumentException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("file is not exist!");
        }
    }


    public PluginConfig initConfig(String context) {
        PluginConfig pluginConfig = new PluginConfig();
        if (context.isEmpty()) {
            return pluginConfig;
        }
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(new ByteArrayInputStream(context.getBytes()));
            Element root = document.getRootElement();
            //common params
            for (Iterator iterator = root.elementIterator("param"); iterator
                    .hasNext(); ) {
                Element param = (Element) iterator.next();

                pluginConfig.putParam(param.attributeValue("name").trim(),
                        param.getText());
            }
            //input datas
            for (Iterator iterator = root.elementIterator("input_data"); iterator
                    .hasNext(); ) {
                Element data = (Element) iterator.next();

                ConfData confData = new ConfData(
                        data.attributeValue("type").trim(),
                        data.attributeValue("link").trim(),
                        data.attributeValue("loadCommand").trim(),
                        data.attributeValue("meta").trim());
                pluginConfig.putInConfData(
                        data.attributeValue("name").trim(), confData);
            }
            //output datas
            for (Iterator iterator = root.elementIterator("output_data"); iterator
                    .hasNext(); ) {
                Element data = (Element) iterator.next();

                ConfData confData = new ConfData(
                        data.attributeValue("type").trim(),
                        data.attributeValue("link").trim(),
                        data.attributeValue("loadCommand").trim(),
                        data.attributeValue("meta").trim());
                pluginConfig.putOutConfData(
                        data.attributeValue("name").trim(), confData);
            }
            //records
            for (Iterator iterator = root.elementIterator("record"); iterator
                    .hasNext(); ) {
                Element record = (Element) iterator.next();
                ConfRecord confRecord = new ConfRecord(
                        record.attributeValue("parseType"),
                        record.attributeValue("fieldDelimiter"),
                        record.attributeValue("recordDelimiter"));
                int i = 0;
                for (Iterator iti = record.elementIterator("field"); iti
                        .hasNext(); i++) {
                    Element field = (Element) iti.next();
                    ConfRecordField confRecordField = new ConfRecordField(i,
                            Boolean.parseBoolean(field.attributeValue("nullable")),
                            Integer.parseInt(field.attributeValue("size")),
                            field.attributeValue("type"));
                    confRecord.putField(
                            field.attributeValue("name").trim(),
                            confRecordField);
                }
                pluginConfig.putConfRecord(record.attributeValue("name"),
                        confRecord);
            }
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return pluginConfig;
    }
}

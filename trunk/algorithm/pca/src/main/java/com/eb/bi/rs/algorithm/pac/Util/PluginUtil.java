package com.eb.bi.rs.algorithm.pac.Util;

import java.util.*;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class PluginUtil {
    private String magicCubeBookPath;
    private String logConf;
    private String outputPath;
    private int startCol = 0;
    private int endCol = -1;
    private boolean bDebug = false;
    private double eigSelectThreshold = 0.84999999999999998D;
    private int groupColIndex = -1;
    private boolean bRotate = false;
    private int rotateTryTimes = 25;
    private boolean bNeedAdjust = false;
    private Map<String, List<Integer>> groupIndexes = null;
    public static final String NO_GROUP_TAG = "all";
    private static PluginUtil instance = null;

    public static PluginUtil getInstance() {
        if (instance == null) {
            instance = new PluginUtil();
        }
        return instance;
    }

    public boolean isDebug() {
        return this.bDebug;
    }

    public String getMagicCubeBookPath() {
        return this.magicCubeBookPath;
    }

    public String getOutputDir() {
        return this.outputPath;
    }

    public String getLogConf() {
        return this.logConf;
    }

    public int getStartCol() {
        return this.startCol;
    }

    public int getEndCol() {
        return this.endCol;
    }

    public double getEigThreshold() {
        return this.eigSelectThreshold;
    }

    public int getGroupColIndex() {
        return this.groupColIndex;
    }

    public boolean isRotate() {
        return this.bRotate;
    }

    public int getRotateTryTimes() {
        return this.rotateTryTimes;
    }

    public boolean isNeedAdjust() {
        return this.bNeedAdjust;
    }

    public Map<String, List<Integer>> getFilterCols() {
        return this.groupIndexes;
    }

    public List<Integer> getFilterIndexes(String key) {
        if (this.groupIndexes == null) {
            return null;
        }
        List indexes = (List) this.groupIndexes.get(key);
        return indexes;
    }

    public void load(String conf) {
        SAXReader reader = new SAXReader();
        Document doc = null;
        try {
            String[] strs;
            doc = reader.read(conf);
            Element rootElem = doc.getRootElement();
            this.magicCubeBookPath = rootElem.elementTextTrim("magicCubeBookPath");
            this.outputPath = rootElem.elementTextTrim("outputPath");
            this.logConf = rootElem.elementTextTrim("logConf");
            String startColStr = rootElem.elementTextTrim("calStartCol");
            String endColStr = rootElem.elementTextTrim("calEndCol");
            String thresholdStr = rootElem.elementTextTrim("eigSelectThreshold");
            String debugStr = rootElem.elementTextTrim("bDebug");
            String groupStr = rootElem.elementTextTrim("groupCol");
            String rotateStr = rootElem.elementTextTrim("rotate");
            String rotateTryTimesStr = rootElem.elementTextTrim("rotateTryTimes");
            String adjustStr = rootElem.elementTextTrim("bAdjust");
            try {
                this.startCol = Integer.parseInt(startColStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if ((endColStr != null) && (endColStr.length() > 0))
                try {
                    this.endCol = Integer.parseInt(endColStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            try {
                int threshold = Integer.parseInt(thresholdStr);
                this.eigSelectThreshold = (1D * threshold / 100.0D);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if ((debugStr != null) && (debugStr.equals("1"))) {
                this.bDebug = true;
            }
            if ((groupStr != null) && (groupStr.length() > 0))
                try {
                    this.groupColIndex = Integer.parseInt(groupStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            if ((rotateStr != null) && (rotateStr.equals("1"))) {
                this.bRotate = true;
            }
            if ((rotateTryTimesStr != null) && (rotateTryTimesStr.length() > 0))
                try {
                    this.rotateTryTimes = Integer.parseInt(rotateTryTimesStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            if ((adjustStr != null) && (adjustStr.equals("1"))) {
                this.bNeedAdjust = true;
            }
            Element groupElem = rootElem.element("filterGroups");
            List groupElements = groupElem.elements("groupIndex");
            if ((groupElements.size() == 1) &&
                    (((Element) groupElements.get(0)).attributeValue
                            ("value").equals("all"))) {
                String[] arrayOfString1;
                String value = ((Element) groupElements.get(0)).getTextTrim();
                strs = value.split(",");
                List indexes = new ArrayList();
                int j = (arrayOfString1 = strs).length;
                for (int i = 0; i < j; ++i) {
                    String s = arrayOfString1[i];

                    indexes.add(Integer.valueOf(Integer.parseInt(s)));
                }
                if (this.groupIndexes == null) {
                    this.groupIndexes = new HashMap();
                }
                this.groupIndexes.put("all", indexes);
            } else {
                for (Iterator<Element> it = groupElements.iterator(); it.hasNext(); ) {
                    String[] arrayOfString2;
                    Element groupIndex = (Element) it.next();

                    String groupKey = groupIndex.attributeValue("value");
                    String value = groupIndex.getTextTrim();
                    strs = value.split(",");
                    Object indexes = new ArrayList();
                    int l = (arrayOfString2 = strs).length;
                    for (int k = 0; k < l; ++k) {
                        String s = arrayOfString2[k];

                        ((List) indexes).add(Integer.valueOf(Integer.parseInt(s)));
                    }
                    if (this.groupIndexes == null) {
                        this.groupIndexes = new HashMap();
                    }
                    this.groupIndexes.put(groupKey, (List)indexes);
                }
            }
            LogUtil logUtil = LogUtil.getInstance();
            logUtil.initLog(this.logConf, "pca");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
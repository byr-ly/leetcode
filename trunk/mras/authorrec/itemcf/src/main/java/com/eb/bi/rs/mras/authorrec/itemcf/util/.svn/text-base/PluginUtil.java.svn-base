package com.eb.bi.rs.mras.authorrec.itemcf.util;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class PluginUtil {

    public class WorkPath {
        String pathValue;
        boolean bParts;
        boolean bVirtual = false;
        boolean bDaily = true;
        boolean bConstruct = false;

        public WorkPath(String path, boolean bparts, boolean bcont) {
            pathValue = path;
            bParts = bparts;
            bConstruct = bcont;
        }

        public String getPathValue() {
            String path = this.pathValue;
            if (bDaily && bParts && !bConstruct) {
                if (path.endsWith("/") || path.endsWith("\\")) {
                    path = String.format("%s%s", path, dateStr);
                } else {
                    path = String.format("%s/%s", path, dateStr);
                }
            }
            return path;
        }

        public String getPathValueLastday() {
            String path = this.pathValue;
            Calendar calendar = Calendar.getInstance();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            try {
                Date date = format.parse(dateStr);
                calendar.setTime(date);
                calendar.add(Calendar.DAY_OF_YEAR, -1);
                Date lastday = calendar.getTime();
                String lastdayStr = format.format(lastday);
                if (path.endsWith("/") || path.endsWith("\\")) {
                    path = String.format("%s%s", path, lastdayStr);
                } else {
                    path = String.format("%s/%s", path, lastdayStr);
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
            return path;
        }

        public boolean getIsParts() {
            return this.bParts;
        }

        public void setIsVirtual(boolean b) {
            this.bVirtual = b;
        }

        public boolean getIsVirtual() {
            return this.bVirtual;
        }

        public void setDaily(boolean b) {
            this.bDaily = b;
        }

        @Override
        public String toString() {
            return pathValue;
        }
    }

    public static String[] DEBUG_USERS = {"13388613966", "13858038966", "15057100189",
            "15205812505", "18806522025", "13567106170", "18868721023"};

    //输入
    //用户-图书打分表
    public static final String USER_BOOK_SCORE_KEY = "user-book-score-file";
    //图书-作者-分类等 bookinfo
    public static final String BOOKINFO_KEY = "bookinfo-file";
    //图书推荐库
    public static final String RECOMM_BOOK_KEY = "recomm-book-file";
    //用户偏好表
    public static final String USER_PREFER_CLASS_KEY = "user-prefer-class-file";
    //待推荐用户-初始化
//	public static final String TORECOMM_USER_INIT_KEY = "torecomm-user-file-init";
    //智能推荐6月累计数据(用户属性)
    public static final String USER_READ_DEPTH_6CM_KEY = "user-read-depth-6cm";
    //智能推荐累计数据（用户图书属性）
    public static final String USER_BOOK_READ_DEPTH_HISTORY_KEY = "user-book-read-depth-history";
    //待推荐用户-更新
    public static final String TORECOMM_USER_INC_KEY = "torecomm-user-file";
    //上一天的推荐展示结果——加入黑名单
    public static final String RECOMM_RESULT_LASTDAY_KEY = "lastday-recomm-display-result-file";
    //旧黑名单
    public static final String RECOMM_BLACKLIST_KEY = "recomm-blacklist-curr-file";
    //用户分群
    public static final String USER_GROUP_KEY = "user-group-file";
    //结果检查
    public static final String BOOK_DETAIL_KEY = "book-detail-file";
    //名家
    public static final String FAMOUS_AUTHOR_KEY = "famous-author-file";

    //入库输出
    //用户-作者打分
    public static final String READ_AUTHOR_SCORE_OUT_KEY = "read-author-score-out";
    //用户-作者预测打分
//	public static final String PREDICT_AUTHOR_SOCRE_OUT_KEY = "user-author-predict-score-out";
    //推荐结果
    public static final String RECOMM_RESULT_OUT_KEY = "recomm-result-out";
    //更新黑名单
    public static final String RECOMM_BLACKLIST_OUT_KEY = "recomm-blacklist-new-out";
    //推荐结果展示
    public static final String RECOMM_DISPLAY_RESULT_OUT_KEY = "recomm-display-result-out";

    //临时输出
    //作者-分类-图书本数
    public static final String TEMP_AUTHOR_CLASS_COUNT_KEY = "author-book-count-out";
    //作者-分类-在架图书本数
    public static final String TEMP_AUTHOR_CLASS_ONSHELF_COUNT_KEY = "author-book-onshelf-count-out";
    //关联图书推荐库的作家分类表
    public static final String TEMP_AUTHOR_CLASSIFY_TABLE_KEY = "recomm-author-filter-out";
    //从打分表中只筛选作家分类表中的作者们，且打分用户数超过最小用户数
//    public static final String TEMP_READ_AUTHOR_FILTER_KEY = "read-author-filter-out";
    //根据作家分类表过滤后的打分表,只筛选推荐库中图书对应的作者们：用户-作者-score
    public static final String TEMP_READ_USER_AUTHOR_SCORE_FILTER_KEY = "read-author-score-filter-out";
    //增量更新时，从过滤后的用户作者打分表中再过滤用户
    public static final String TEMP_INC_READ_USER_AUTHOR_FILTER_KEY = "inc-read-author-score-filter-out";
    //协同过滤——筛选深度用户
    public static final String TEMP_DEEP_USER_GROUP_KEY = "deep-user-group-out";
    //协同过滤——筛选深度用户
    public static final String TEMP_DEEP_USER_GROUP_6CM_KEY = "deep-user-group-6cm-out";
    //协同过滤——用户i-用户j-scorei-scorej
    public static final String TEMP_USER_AUTHOR_SCORE_MERGE_KEY = "user-author-score-merge-out";
    //协同过滤——作者相似性
    public static final String TEMP_AUTHOR_SIMILARITY_KEY = "author-similarity-out";
    //协同过滤——作者平均分
    public static final String TEMP_AUTHOR_AVG_SCORE_KEY = "author-avg-score-out";
    //协同过滤——作者重编号
    public static final String TEMP_COLL_AUTHOR_REINDEX_KEY = "coll-author-reindex-out";
    //用户打分表split
    public static final String TEMP_AUTHOR_REINDEX_SCORE_FILTER_OUT = "author-reindex-score-filter-out";
    //用户偏好表split
    public static final String TEMP_USER_PREF_CLASS_SPLIT_OUT = "user-pref-class-split-out";
    //协同预测过滤（用户-作者-打分）
    public static final String TEMP_COLL_PREDICT_FILTER_SCORE_OUT = "coll-predict-filter-score-out";
    //datajoin
    public static final String TEMP_DATAJOIN_OUT1 = "predict-join-part1-out";
    public static final String TEMP_DATAJOIN_OUT2 = "predict-join-part2-out";
    //debug
    public static final String TEMP_DEBUG_SCORE_FILTER_OUT = "debug-score-filter-out";
    //检查结果
    public static final String TEMP_DEBUG_CHECK_RESULT_OUT = "debug-check-result-out";

    //拆分作者相似性作为输入，临时结果
    public static final String TEMP_PREDICT_SCORE_PARTS_KEY = "predict-score-parts-out";
    //初始化待推荐用户重排
    public static final String TEMP_REORDER_INIT_USER_KEY = "reorder_init_user_out";
    //阅读深度重排
    public static final String TEMP_REORDER_READ_DEPTH_KEY = "reorder_read_depth_out";
    //预测评分中，关联用户偏好筛选作者
    public static final String TEMP_PREDICT_FILTER_AUTHOR_PREF_KEY = "user-prefer-author-filter-out";
    //打分作者推荐前过滤
    public static final String TEMP_TORECOMM_USER_READ_KEY = "torecomm-read-socre-filter-out";
    //预测打分作者推荐前过滤
    public static final String TEMP_TORECOMM_USER_PREIDICT_KEY = "torecomm-predict-socre-filter-out";
    //补白推荐用户过滤
    public static final String TEMP_TORECOMM_USER_FILLER_KEY = "torecomm-filler-user-pref-filter-out";
    //作者所写图书较多的分类
    public static final String TEMP_AUTHOR_BIG_CLASS_KEY = "author-big-class-out";
    //从偏好表中筛选待推荐的用户
    public static final String TEMP_TORECOMM_USER_PREF_FILTER_KEY = "torecomm_user_pref_filter_out";
    //已读作者推荐中图书深度过滤
    public static final String TEMP_DEEP_READ_FILTER_READ_KEY = "deep-read-filter-forread-out";
    //补白用户推荐中图书深度过滤
    public static final String TEMP_DEEP_READ_FILTER_FILLER_KEY = "deep-read-filter-forfiller-out";
    //关联作家分类表筛选名家
    public static final String TEMP_AUTHOR_FAMOUS_BOOK_KEY = "torecomm-famous-author-book-out";
    //打分作者图书推荐结果——output
    public static final String TEMP_RECOMM_READ_RESULT_KEY = "recomm-read-result-out";
    //预测作者图书推荐结果——output(协同的结果)
    public static final String TEMP_RECOMM_PREDILCT_RESULT_KEY = "recomm-predict-result-out";
    //预测作者图书推荐结果——output(推荐时候根据用户分片)
    public static final String TEMP_RECOMM_PREDICT_RESULT_ORDER_KEY = "recomm-predict-result-order-out";
    //补白作者图书推荐结果——output
    public static final String TEMP_RECOMM_FILLER_RESULT_KEY = "recomm-filler-result-out";
    //不补白用户（预测作者足够）
    public static final String TEMP_NO_FILLER_USER_KEY = "no-filler-user-out";


    //

    //
    public static String USE_NEW_HADOOP_KEY = "bUseNewHadoop";
    public static String INC_UPDATE_KEY = "bIncrementUpdate";
    public static String PREF_CLASS_SIZE_KEY = "prefClassSize";
    public static String BOOK_SCORE_MIN_KEY = "bookScoreMin";
    public static String SIM_AUTHOR_COMM_USER_MIN_KEY = "authorCommUserMin";
    public static String RECOMM_AUTHOR_NUM = "authorRecommNum";
    public static String RECOMM_AUTHOR_NUM_READ = "readAuthorRecommNum";
    public static String RECOMM_AUTHOR_NUM_TYPE4 = "type4AuthorRecommNum";
    public static String COLL_USE_LOGIN_USER = "collUseLoginUser";
    public static String COLL_USE_DEEP_USER = "collUseDeepUser";
    public static String INIT_USER_TURN_PART = "initUserTurnParts";
    public static String COLL_PREDICT_USER_SPLIT = "collSplitUserScore";
    public static String DEBUG = "bDebug";
    public static String COLL_AUTHOR_SIM_MIN = "collAuthorSimMin";
    public static String COLL_USER_SPLIT_NUM = "collUserSplitNum";
    public static String USER_SPLIT_NUM = "userSplitNum";
    public static String COLL_PARALLE_NUM = "collParallelNum";

    private String logConf;
    private int prefClassSize = 3;
    private int totalRecommNum = 9;
    private int readRecommNum = 2;
    private int type4RecommNum = 2;
    private int reduceNum = 10;
    private int reduceNumBig = 20;
    private double bookScoreMin = 0;
    private int authorCommUserMin = 10;
    private boolean bIncrementUpdate = true;
    private boolean bUseNewHadoop = true;
    private boolean bCollUseLoginUser = false;
    private boolean bCollUseDeepUser = true;
    private int initUserTurnPart = 10;
    private boolean bCollPredictSplitUser = false;
    private boolean bDebug = false;
    private float authorSimMin = 0;
    private int colluserSplitNum = 100;
    private int userSplitNum = -1;
    private int collParalleNum = 1;

    private String dateStr = "";

    private Map<String, WorkPath> paraPathMap = new HashMap<String, WorkPath>();

    private static PluginUtil instance = null;

    public static PluginUtil getInstance() {
        if (instance == null) {
            instance = new PluginUtil();
        }
        return instance;
    }

    private PluginUtil() {

    }

    public String getDateStr() {
        return this.dateStr;
    }

    public String getLogConf() {
        return this.logConf;
    }

    public int getPrefClassSize() {
        return this.prefClassSize;
    }

    public int getTotalRecommNum() {
        return this.totalRecommNum;
    }

    public int getReadRecommNum() {
        return this.readRecommNum;
    }

    public int getType4RecommNum() {
        return this.type4RecommNum;
    }

    public int getReduceNum() {
        return this.reduceNum;
    }

    public int getReduceNumBig() {
        return this.reduceNumBig;
    }

    public double getBookScoreMin() {
        return this.bookScoreMin;
    }

    public int getAuthorCommUserMin() {
        return this.authorCommUserMin;
    }

    public boolean isIncrementUpdate() {
        return this.bIncrementUpdate;
    }

    public boolean isNewHadoop() {
        return this.bUseNewHadoop;
    }

    public boolean isCollUseLoginUser() {
        return this.bCollUseLoginUser;
    }

    public boolean isCollUseDeepUser() {
        return this.bCollUseDeepUser;
    }

    public int getInitUserPart() {
        return this.initUserTurnPart;
    }

    public boolean isCollPredictSplitUser() {
        return this.bCollPredictSplitUser;
    }

    public boolean isDebug() {
        return this.bDebug;
    }

    public float getAuthorSimMin() {
        return this.authorSimMin;
    }

    public int getCollUserSplitNum() {
        return colluserSplitNum;
    }

    public int getRecommUserSplitNum() {
        return userSplitNum;
    }

    public int getCollParalleNum() {
        return collParalleNum;
    }

    public void load(String conf, String date) {
        if (date == null) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            dateStr = format.format(new Date());
        } else {
            dateStr = date;
        }
        //	dateStr = "20150617";
//		File exeFile = new File("");
//		String exePath = exeFile.getAbsolutePath();
        SAXReader reader = new SAXReader();
        Document doc = null;
        try {
            doc = reader.read(conf);
            Element rootElem = doc.getRootElement();
            this.logConf = rootElem.elementTextTrim("logConf");
            String prefStr = rootElem.elementTextTrim(PREF_CLASS_SIZE_KEY);
            this.prefClassSize = getIntValue(prefStr, prefClassSize);
            String recommNumStr = rootElem.elementTextTrim(RECOMM_AUTHOR_NUM);
            this.totalRecommNum = getIntValue(recommNumStr, totalRecommNum);
            recommNumStr = rootElem.elementTextTrim(RECOMM_AUTHOR_NUM_READ);
            this.readRecommNum = getIntValue(recommNumStr, readRecommNum);
            recommNumStr = rootElem.elementTextTrim(RECOMM_AUTHOR_NUM_TYPE4);
            this.type4RecommNum = getIntValue(recommNumStr, type4RecommNum);
            String reduceNumStr = rootElem.elementTextTrim("reduceNum");
            this.reduceNum = getIntValue(reduceNumStr, reduceNum);
            String bigReduceNumStr = rootElem.elementTextTrim("reduceNumBig");
            this.reduceNumBig = getIntValue(bigReduceNumStr, reduceNumBig);
            String scoreMinStr = rootElem.elementTextTrim(BOOK_SCORE_MIN_KEY);
            this.bookScoreMin = getDoubleValue(scoreMinStr, bookScoreMin);
            String authorCommUserMinStr = rootElem.elementTextTrim(SIM_AUTHOR_COMM_USER_MIN_KEY);
            this.authorCommUserMin = getIntValue(authorCommUserMinStr, authorCommUserMin);
            String incrementStr = rootElem.elementTextTrim(INC_UPDATE_KEY);
            if (incrementStr != null && incrementStr.equals("0")) {
                this.bIncrementUpdate = false;
            }
            String newHadoopStr = rootElem.elementTextTrim(USE_NEW_HADOOP_KEY);
            if (newHadoopStr != null && newHadoopStr.equals("0")) {
                this.bUseNewHadoop = false;
            }
            String useLoginUserStr = rootElem.elementTextTrim(COLL_USE_LOGIN_USER);
            if (useLoginUserStr != null && useLoginUserStr.equals("0")) {
                this.bCollUseLoginUser = false;
            }
            String collSplitUserStr = rootElem.elementTextTrim(COLL_PREDICT_USER_SPLIT);
            if (collSplitUserStr != null && collSplitUserStr.equals("1")) {
                this.bCollPredictSplitUser = true;
            }
            String debugStr = rootElem.elementTextTrim(DEBUG);
            if (debugStr != null && debugStr.equals("1")) {
                bDebug = true;
            }
            String authorSimMinStr = rootElem.elementTextTrim(COLL_AUTHOR_SIM_MIN);
            if (authorSimMinStr != null) {
                try {
                    authorSimMin = Float.parseFloat(authorSimMinStr);
                } catch (Exception e) {
                    // TODO: handle exception
                }
            }
            String collParaStr = rootElem.elementTextTrim(COLL_PARALLE_NUM);
            collParalleNum = getIntValue(collParaStr, collParalleNum);
            String userSplitStr = rootElem.elementTextTrim(COLL_USER_SPLIT_NUM);
            colluserSplitNum = getIntValue(userSplitStr, colluserSplitNum);
            userSplitStr = rootElem.elementTextTrim(USER_SPLIT_NUM);
            userSplitNum = getIntValue(userSplitStr, userSplitNum);
            String initPartStr = rootElem.elementTextTrim(INIT_USER_TURN_PART);
            this.initUserTurnPart = getIntValue(initPartStr, initUserTurnPart);
            Element inputElem = rootElem.element("inputs");
            initPathValues(inputElem, false);
            Element saveOutElem = rootElem.element("saveOutputs");
            initPathValues(saveOutElem, true);
            Element tempOutElem = rootElem.element("tempOutputs");
            initPathValues(tempOutElem, true);

            LogUtil logUtil = LogUtil.getInstance();
            logUtil.initLog(logConf, "authorRecomm");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int getIntValue(String str, int origValue) {
        if (str != null && str.length() > 0) {
            try {
                int value = Integer.parseInt(str);
                return value;
            } catch (Exception e) {
                return origValue;
            }
        }
        return origValue;
    }

    private double getDoubleValue(String str, double origValue) {
        if (str != null && str.length() > 0) {
            try {
                double value = Double.parseDouble(str);
                return value;
            } catch (Exception e) {
                return origValue;
            }
        }
        return origValue;
    }

    private void initPathValues(Element parentElement, boolean bParts) {
        List<Element> elems = parentElement.elements();
        for (Element element : elems) {
            String attr = element.attributeValue("bDaily");
            String key = element.getName();
            String value = element.getTextTrim();
            WorkPath workPath = new WorkPath(value, bParts, false);
            if (attr != null && attr.equals("0")) {
                workPath.setDaily(false);
            }
            paraPathMap.put(key, workPath);
        }
    }

    public WorkPath getPath(String key) {
        return paraPathMap.get(key);
    }
}

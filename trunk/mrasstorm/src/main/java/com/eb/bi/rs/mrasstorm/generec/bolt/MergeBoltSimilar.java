package com.eb.bi.rs.mrasstorm.generec.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.hbase.scan.HbaseScannerByTime;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mrasstorm.generec.domain.BookGeneClassInfo;
import com.eb.bi.rs.mrasstorm.generec.domain.BookInfo;
import com.eb.bi.rs.mrasstorm.generec.domain.BookSeriesInfo;
import com.eb.bi.rs.mrasstorm.generec.domain.SortByScore;
import com.eb.bi.rs.mrasstorm.generec.util.MergeUtils;
import com.eb.bi.rs.mrasstorm.generec.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用于处理推荐结果：混合，过滤，排序，补白
 *
 * @author ynn
 * @version 1.0
 * @revised by liyang
 * @date 创建时间：2015-10-20 上午11:00:50
 */
public class MergeBoltSimilar extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;

    private HTableInterface userPrefTable; // 用户偏好图书表
    private static final byte[] REPOCF = Bytes.toBytes("cf"); // hbase 列族
    private static final byte[] REPOCQSCORE = Bytes.toBytes("c"); // hbase 列名

    private HTableInterface userRealTable;// 实时图书行为表

    private HTableInterface baseScoreTable;// 图书基础分表
    private static final byte[] REPOCQCLASS = Bytes.toBytes("class");
    private static final byte[] REPOCQSERIESID = Bytes.toBytes("seriesid");
    private static final byte[] REPOCQORDERID = Bytes.toBytes("orderid");
    private static final byte[] REPOCQAUTHORID = Bytes.toBytes("authorid");

    private HTableInterface editScoreTable;// 图书编辑分表
    private HTableInterface bookFillerTable;// 图书补白表
    private HTableInterface userHisTable;// 历史图书行为及黑名单表
    private HTableInterface bookBlackListTable;// 图书黑名单表
    //add by liyang 过滤统一瀑布流结果
    private HTableInterface unifyRecomTable;//统一瀑布流结果表
    private HTableInterface geneTypeTable;//基因分类表
    private HTableInterface bookStatusTable;//图书状态表

    private HTableInterface recRepTable; // 推荐库 图书ID|定制标签|版面集

    //for test
//    private HTableInterface baseScoreTest;
//    private HTableInterface unifyResultTest;
//    private HTableInterface prefScoreTest;

    private Jedis respJedis;
    private String respTable;
    private int respExpireTime;


    // 图书基础分，编辑分，状态
    private Map<String, Float> baseScoreMap = new ConcurrentHashMap<String, Float>();
    private Map<String, String> bookClassMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
    private Map<String, Float> editScoreMap = new ConcurrentHashMap<String, Float>();
    private Map<String, BookInfo> bookInfoMap = new ConcurrentHashMap<String, BookInfo>();

    // 补白
    private Map<String, String> allBookFilterMap = new HashMap<String, String>();
    private Map<String, String> allBookFilterCopyMap = new HashMap<String, String>();

    //在架图书表
    private Map<String, String> bookStatusMap = new HashMap<String, String>();
    // 图书黑名单
    private Vector<String> bookBlackVector = new Vector<String>();
    private static int sameAuthorCount = 0;

    private static float baseScorePer = 0; // 各个部分的权重值，基础分，偏好分，相似分
    private static float editScorePer = 0;
    private static float prefScorePer = 0;
    private static float similarScorePer = 0;

    private static String editScoreFixed = null;

    private static int hour = 0;
    private static int topN = 0;
    private HashSet<String> behaviorSet = new HashSet<String>(); // order or read or pv

    // 定时器
    private transient Thread loader = null;

    private boolean isFirstLoad = true;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        PluginConfig appConfig = ConfigReader.getInstance().initConfig(
                stormConf.get("AppConfig").toString());

        baseScorePer = Float.parseFloat(appConfig.getParam("baseScorePer"));
        editScorePer = Float.parseFloat(appConfig.getParam("editScorePer"));
        prefScorePer = Float.parseFloat(appConfig.getParam("prefScorePer"));
        similarScorePer = Float.parseFloat(appConfig.getParam("similarScorePer"));

        sameAuthorCount = Integer.parseInt((appConfig.getParam("same_author_count")));
        editScoreFixed = appConfig.getParam("editScore");
        topN = Integer.parseInt((appConfig.getParam("topN")));
        hour = Integer.parseInt((appConfig.getParam("hour_base")));

        String behaviorStr = appConfig.getParam("user_behavior"); // order,read,pv
        String[] behs = behaviorStr.split(",");
        for (String b : behs) {
            behaviorSet.add(b);
        }

        respTable = appConfig.getParam("response_table");
        respExpireTime = Integer.parseInt(appConfig.getParam("resp_expire_time"));

        String addrs[] = appConfig.getParam("resp_redis").split("::");
        respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
        respJedis.auth(addrs[2]);
        while (respJedis == null) {
            respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
            respJedis.auth(addrs[2]);
        }

        // 初始化Hbase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                appConfig.getParam("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",
                appConfig.getParam("hbase.zookeeper.property.clientPort"));
        HConnection con = null;
        while (con == null) {
            try {
                con = HConnectionManager.createConnection(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户图书偏好表
        while (userPrefTable == null) {
            try {
                userPrefTable = con.getTable(appConfig
                        .getParam("engine_attribute_result"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户实时图书行为表
        while (userRealTable == null) {
            try {
                userRealTable = con.getTable(appConfig
                        .getParam("realpub_user_behavior"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户历史图书行为及黑名单表
        while (userHisTable == null) {
            try {
                userHisTable = con.getTable(appConfig
                        .getParam("user_read_history"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书基础分表
        while (baseScoreTable == null) {
            try {
                baseScoreTable = con.getTable(appConfig
                        .getParam("unify_base_score"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书编辑分表
        while (editScoreTable == null) {
            try {
                editScoreTable = con.getTable(appConfig
                        .getParam("unify_edit_score"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书补白库表
        while (bookFillerTable == null) {
            try {
                bookFillerTable = con.getTable(appConfig
                        .getParam("tages_filler"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书黑名单表
        while (bookBlackListTable == null) {
            try {
                bookBlackListTable = con.getTable(appConfig
                        .getParam("unify_book_blacklist"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //统一瀑布流结果表
        while (unifyRecomTable == null) {
            try {
                unifyRecomTable = con.getTable(appConfig
                        .getParam("realpub_recommend_buffer"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //基因分类结果表
        while (geneTypeTable == null) {
            try {
                geneTypeTable = con.getTable(appConfig
                        .getParam("gene_type_result"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //图书状态表
        while (bookStatusTable == null) {
            try {
                bookStatusTable = con.getTable(appConfig
                        .getParam("book_status"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        while (recRepTable == null) {
            try {
                recRepTable = con.getTable(appConfig
                        .getParam("unified_rec_rep"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        //for test
//        while (baseScoreTest == null) {
//            try {
//                baseScoreTest = con.getTable(appConfig
//                        .getParam("base_score_test"));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        while (unifyResultTest == null) {
//            try {
//                unifyResultTest = con.getTable(appConfig
//                        .getParam("unify_result_test"));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        while (prefScoreTest == null) {
//            try {
//                prefScoreTest = con.getTable(appConfig
//                        .getParam("pref_score_test"));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }


        // 定时器，每天4点从HDFS上读取图书基础分、编辑分
        if (loader == null) {
            loader = new Thread(new Runnable() {
                public void run() {
                    while (true) {
                        baseScoreMap.clear();
                        bookClassMap.clear();
                        bookInfoMap.clear();
                        bookStatusMap.clear();
                        try {
                            //Scan scan1 = new Scan();
                            //scan1.setCaching(100);
                            // 设置查询数据的时间范围
                        	//long maxStamp = System.currentTimeMillis();
                        	//long minStamp = maxStamp - 24 * 60 * 60 * 1000;
                        	//scan1.setTimeRange(minStamp, maxStamp);
                            // 初始化图书基础分及图书所属分类
                            //ResultScanner rs = baseScoreTable.getScanner(scan1);
                        	List<Result> resultList = new ArrayList<Result>();
                        	resultList = HbaseScannerByTime.getFillerBook(baseScoreTable, 10000, 10);
                            for (Result result : resultList) {
                                String bookId = Bytes.toString(result.getRow());
                                String score = Bytes.toString(result.getValue(
                                        REPOCF, REPOCQSCORE));
                                String cls = Bytes.toString(result.getValue(
                                        REPOCF, REPOCQCLASS));
                                String seriesStr = Bytes.toString(result
                                        .getValue(REPOCF, REPOCQSERIESID));
                                String orderStr = Bytes.toString(result
                                        .getValue(REPOCF, REPOCQORDERID));
                                String authorId = Bytes.toString(result
                                        .getValue(REPOCF, REPOCQAUTHORID));
                                try {
                                    if (score == null || "".equals(score)) {
                                        score = "0";
                                    }
                                    if (seriesStr == null
                                            || "".equals(seriesStr)) {
                                        seriesStr = "0";
                                    }
                                    if (orderStr == null || "".equals(orderStr)) {
                                        orderStr = "0";
                                    }
                                    float baseScore = Float.parseFloat(score);
                                    int seriesId = Integer.parseInt(seriesStr);
                                    int orderId = Integer.parseInt(orderStr);
                                    baseScoreMap.put(bookId, baseScore);
                                    bookClassMap.put(bookId, cls);
                                    bookInfoMap.put(bookId, new BookInfo(bookId, seriesId,
                                            orderId, authorId));
                                } catch (Exception e) {
                                    PrintHelper
                                            .print("parse base score fail...");
                                    e.printStackTrace();
                                    continue;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        PrintHelper.print("GeneMergeBolt baseScoreMap.size():"
                                + baseScoreMap.size());
                        PrintHelper.print("GeneMergeBolt bookClassMap.size():"
                                + bookClassMap.size());

                        // 初始化图书编辑分
                        try {
                            ResultScanner rs2 = null;
                            Scan scan2 = new Scan();
                            scan2.setCaching(100);
                            rs2 = editScoreTable.getScanner(scan2);
                            for (Result result : rs2) {
                                try {
                                    String bookId = Bytes.toString(result
                                            .getRow());
                                    String scoreStr = Bytes.toString(result
                                            .getValue(REPOCF, REPOCQSCORE));
                                    float editScore = 0;
                                    if (scoreStr == null || scoreStr.equals("")) {
                                        editScore = 0;
                                    } else {
                                        if (editScoreFixed != null) {    //配置文件有该项则取配置文件中的固定值
                                            editScore = Float.parseFloat(editScoreFixed);
                                        } else {
                                            editScore = Float.parseFloat(scoreStr);
                                        }
                                    }
                                    editScoreMap.put(bookId, editScore);
                                } catch (Exception e) {
                                    PrintHelper
                                            .print("parse edit score fail...");
                                    e.printStackTrace();
                                    continue;
                                }

                            }
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        PrintHelper.print("GeneMergeBolt editScoreMap.size():"
                                + editScoreMap.size());

                        // 初始化补白列表
                        allBookFilterMap = getFillerBook();
                        PrintHelper.print("GeneMergeBolt allBookFilterMap.size():"
                                + allBookFilterMap.size());

                        //初始化补白备份列表
                        allBookFilterCopyMap = getFillerBook();
                        PrintHelper.print("GeneMergeBolt allBookFilterCopyMap.size():"
                                + allBookFilterCopyMap.size());

                        //初始化在架图书列表
                        try {
                            bookStatusMap = getBookStatus(bookStatusTable);
                            PrintHelper.print("GeneMergeBolt user: "
                                    + " bookStatusMap size:" + bookStatusMap.size());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        bookBlackVector = getBookBlackList();
                        PrintHelper.print("GeneMergeBolt bookBlackVector.size():"
                                + bookBlackVector.size());

                        long sleepTime = TimeUtil
                                .getMillisFromNowToTwelveOclock(hour);
                        if (sleepTime > 0) {
                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
            loader.setDaemon(true);
            loader.start();
        }
    }

    /*
     * input：user,gene1|gene2|...,bookid,score|bookid,score|bookid,score
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String userId = input.getStringByField("user");
        String geneId = input.getStringByField("gene");
        String bidScoreStr = input.getStringByField("book_scores");
        PrintHelper.print("GeneMergeBolt receive userId :" + userId + " geneId:"
                + geneId);

        Map<String, Float> similarScoreMap = new HashMap<String, Float>();
        if (bidScoreStr != null && !bidScoreStr.equals("")) {
            String[] bidScores = bidScoreStr.split("\\|");
            for (String bidScore : bidScores) {
                String[] tmp = bidScore.split(",");
                if (tmp.length == 2) {
                    similarScoreMap.put(tmp[0], Float.parseFloat(tmp[1]));
                }
            }
        }
        PrintHelper.print("GeneMergeBolt receive user: " + userId
                + " similarScoreMap:" + similarScoreMap.size());

        Map<String, String> bookFilterMap = new HashMap<String, String>();
        Map<String, String> readBookFilterMap = new HashMap<String, String>();

        try {
            Map<String, Float> prefScoreMap = getPrefScore(userId); // 从偏好表中查该User的图书偏好
            PrintHelper.print("GeneMergeBolt receive user: " + userId
                    + " prefScoreMap:" + prefScoreMap.size());

            // 过滤历史图书行为、黑名单、实时图书行为
            Set<String> filterBooksSet = filterBooks(userId, similarScoreMap, prefScoreMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " filterSet size:" + filterBooksSet.size());

            // 混合
            Map<String, Float> bookScoreMap = mixtureCaculate(userId, similarScoreMap, prefScoreMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after mixtureCaculate size:" + bookScoreMap.size());

            // filter series books
            bookScoreMap = filterSeriesBooks(bookScoreMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after filterSeriesBooks size:" + bookScoreMap.size());

            // 过滤同作者
            bookScoreMap = filterSameAuthor(bookScoreMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after filter the same author size:" + bookScoreMap.size());

            // add by liyang 过滤统一瀑布流的结果
            Set<String> unifyBookSet = getUnifyBook(userId);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " UnifyBooks size:" + unifyBookSet.size());

            bookScoreMap = filterUnifyBooks(bookScoreMap, unifyBookSet);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after filterUnifyBooks size:" + bookScoreMap.size());

            //过滤非在架图书
            bookScoreMap = filterUnavailableBook(bookScoreMap, bookStatusMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after filterUnavailableBook size:" + bookScoreMap.size());

            //过滤补白库中历史图书行为、黑名单、实时图书行为以及统一瀑布流结果
            bookFilterMap = bookFilterOne(allBookFilterMap, filterBooksSet, unifyBookSet);
            //bookFilterMap = bookFilterTwo(bookFilterMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after filter fillerSet size:" + bookFilterMap.size());

            //获取补白库中被过滤的书
            readBookFilterMap = getFilterBook(allBookFilterCopyMap, bookFilterMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " readBookFilterMap size:" + readBookFilterMap.size());
            Map<String, ArrayList<String>> readGeneBookListMap = getGeneBookList(readBookFilterMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after readGeneBookListMap size:" + readGeneBookListMap.size());
            Iterator<Entry<String, ArrayList<String>>> aiterator = readGeneBookListMap
                    .entrySet().iterator();
            while (aiterator.hasNext()) {
                Entry<String, ArrayList<String>> entry = aiterator.next();
                String gene = entry.getKey();
                ArrayList<String> bookList = readGeneBookListMap.get(gene);
                PrintHelper.print("GeneMergeBolt user: " + userId
                        + "gene:" + gene + " each gene book size:" + bookList.size());
            }

            Map<String, ArrayList<String>> geneBookListMap = getGeneBookList(bookFilterMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after geneBookListMap size:" + geneBookListMap.size());
            Iterator<Entry<String, ArrayList<String>>> iterator = geneBookListMap
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, ArrayList<String>> entry = iterator.next();
                String gene = entry.getKey();
                ArrayList<String> bookList = geneBookListMap.get(gene);
                PrintHelper.print("GeneMergeBolt user: " + userId
                        + "gene:" + gene + " each gene book size:" + bookList.size());
            }

            //获取基因-分类
            Map<String, ArrayList<String>> geneTypeMap = getGeneType(geneId, geneTypeTable);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after geneTypeMap size:" + geneTypeMap.size());

            //过滤分类不属于用户所选基因的图书
            Map<String, ArrayList<BookGeneClassInfo>> bookGeneScoreMap = filterAccordGene(bookScoreMap, bookClassMap, geneTypeMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after bookGeneScoreMap size:" + bookGeneScoreMap.size());
            Iterator<Entry<String, ArrayList<BookGeneClassInfo>>> iter = bookGeneScoreMap
                    .entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, ArrayList<BookGeneClassInfo>> entry = iter.next();
                String gene = entry.getKey();
                ArrayList<BookGeneClassInfo> list = bookGeneScoreMap.get(gene);
                PrintHelper.print("GeneMergeBolt user: " + userId
                        + "gene:" + gene + " get recom list size:" + list.size());
            }


            //排序并取top
            bookGeneScoreMap = getBookPerGene(bookGeneScoreMap, geneId, geneBookListMap, bookClassMap);
            Iterator<Entry<String, ArrayList<BookGeneClassInfo>>> iter1 = bookGeneScoreMap
                    .entrySet().iterator();
            while (iter1.hasNext()) {
                Entry<String, ArrayList<BookGeneClassInfo>> entry = iter1.next();
                String gene = entry.getKey();
                ArrayList<BookGeneClassInfo> list = bookGeneScoreMap.get(gene);
                PrintHelper.print("GeneMergeBolt user: " + userId
                        + "gene:" + gene + " each gene book size:" + list.size());
            }

 
            ArrayList<BookGeneClassInfo> finalList = getTop(bookGeneScoreMap, bookClassMap,
                    geneId, readGeneBookListMap, geneBookListMap, bookStatusMap);
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after getTop size:" + finalList.size());

            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < finalList.size(); i++) {
                String book = finalList.get(i).getBook();
                String type = finalList.get(i).getType();
                String gene = finalList.get(i).getGene();
                float score = finalList.get(i).getScore();
                sb.append(book + "," + type + "," + gene + "," + score + "|");
            }
            if (sb.toString().endsWith("|")) {
                sb.deleteCharAt(sb.length() - 1);
            }
            //推荐结果中补白图书的分类有可能为null，因为是在基础分表中查的图书分类
            String returnCode = respJedis.setex(respTable + ":" + userId + ":" + geneId,
                    respExpireTime, sb.toString());
            PrintHelper.print("GeneMergeBolt user: " + userId
                    + " after save to redis return :" + returnCode);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * 从偏好表中查该User的图书偏好,rowkey:userid,value:book1,分1|book2,分2|book3,分3|book4,分4
     */
    public Map<String, Float> getPrefScore(String userId) {
        Map<String, Float> prefScMap = new HashMap<String, Float>();
        Get get = new Get(Bytes.toBytes(userId));
        Result result = null;
        try {
            //result = userPrefTable.get(get);

            result = userPrefTable.get(get);
            String bookScores = Bytes.toString(result.getValue(REPOCF,
                    REPOCQSCORE));
            if (bookScores != null) {
                String[] bookScore = bookScores.split("\\|");
                for (int i = 0; i < bookScore.length; i++) {
                    String str = bookScore[i];
                    String[] tmp = str.split(",");
                    if (tmp.length == 2) {
                        float prefScore = Float.parseFloat(tmp[1]);
                        prefScMap.put(tmp[0], prefScore);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prefScMap;
    }

    /*
     * 过滤 不属于对应版面的图书及历史图书行为、黑名单、实时图书行为
     *
     * @return 返回所有需要过滤的图书
     */
    private Set<String> filterBooks(String userId,
                                    Map<String, Float> similarScoreMap, Map<String, Float> prefScoreMap) {

        Set<String> realBooks = getUserRealBehavior(userId); // 实时图书
        Set<String> hisBlackBooks = getUserHisBlackBooks(userId); // 历史图书、黑名单

        Iterator<Entry<String, Float>> iterator = similarScoreMap
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Float> entry = iterator.next();
            String book = entry.getKey();
            if (realBooks.contains(book)) {
                iterator.remove();
                continue;
            }
            if (hisBlackBooks.contains(book)) {
                iterator.remove();
                continue;
            }
            if (bookBlackVector.contains(book)) {
                iterator.remove();
            }
        }
        PrintHelper.print("GeneMergeBolt user: " + userId
                + " after similarScore filter size:" + similarScoreMap.size());

        iterator = prefScoreMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Float> entry = iterator.next();
            String book = entry.getKey();
            if (realBooks.contains(book)) {
                iterator.remove();
                continue;
            }
            if (hisBlackBooks.contains(book)) {
                iterator.remove();
                continue;
            }
            if (bookBlackVector.contains(book)) {
                iterator.remove();
            }
        }
        PrintHelper.print("GeneMergeBolt user: " + userId
                + " after prefScore filter size:" + prefScoreMap.size());

        // 所有需要过滤的图书
        realBooks.addAll(hisBlackBooks);
        return realBooks;
    }

    /*
     * 混合计算图书基础分、编辑分、相似分、偏好分,编辑分暂不参与计算，即编辑分权重为0
     */
    private Map<String, Float> mixtureCaculate(String userId,
                                               Map<String, Float> similarScoreMap, Map<String, Float> prefScoreMap) {
        Map<String, Float> bookScoreMap = new HashMap<String, Float>();
        // 计算相似Map中所有图书的混合分
        Set<String> similarKeySet = similarScoreMap.keySet();
        Iterator<String> it = similarKeySet.iterator();
        float tempScore = 0;
        while (it.hasNext()) {
            String book = it.next();
            if (prefScoreMap.containsKey(book)) { // 偏好分Map中有对应的book
                tempScore = prefScoreMap.get(book) * prefScorePer
                        + similarScoreMap.get(book) * similarScorePer;
                prefScoreMap.remove(book);// 从偏好Map中删除在相似Map中有的图书
                if (baseScoreMap.containsKey(book)) { // 不在推荐库中的图书过滤掉
                    tempScore += baseScoreMap.get(book) * baseScorePer;
                    if (editScoreMap.containsKey(book)) {
                        tempScore += editScoreMap.get(book) * editScorePer;
                    }
                    bookScoreMap.put(book, tempScore);
                }
                tempScore = 0;
            } else {// 偏好分Map中没有对应的book
                tempScore = similarScoreMap.get(book) * similarScorePer;
                if (baseScoreMap.containsKey(book)) { // 不在推荐库中的图书过滤掉
                    tempScore += baseScoreMap.get(book) * baseScorePer;
                    if (editScoreMap.containsKey(book)) {
                        tempScore += editScoreMap.get(book) * editScorePer;
                    }
                    bookScoreMap.put(book, tempScore);
                }
                tempScore = 0;
            }
        }

        // 计算在偏好Map中但不在相似Map中的图书混合分数
        Set<String> prefKeySet = prefScoreMap.keySet();
        it = prefKeySet.iterator();
        tempScore = 0;
        while (it.hasNext()) {
            String prefBook = it.next();
            tempScore = prefScoreMap.get(prefBook) * prefScorePer;
            if (baseScoreMap.containsKey(prefBook)) { // 不在推荐库中的图书过滤掉
                tempScore += baseScoreMap.get(prefBook) * baseScorePer;
                if (editScoreMap.containsKey(prefBook)) {
                    tempScore += editScoreMap.get(prefBook) * editScorePer;
                }
                bookScoreMap.put(prefBook, tempScore);
            }
            tempScore = 0;
        }
        return bookScoreMap;
    }

    /*
      * 过滤同系列图书
      */
    private Map<String, Float> filterSeriesBooks(Map<String, Float> bookScoreMap) {
        Map<String, Float> nonSerialBookMap = new HashMap<String, Float>();
        Map<Integer, BookInfo> serialBookMap = new HashMap<Integer, BookInfo>();
        Set<String> bookSet = bookScoreMap.keySet();
        float maxScore = 0;
        for (String book : bookSet) {
            if (bookInfoMap.containsKey(book)) {
                BookInfo bookInfo = bookInfoMap.get(book);
                int seriesId = bookInfo.getSeriesId();
                int orderId = bookInfo.getOrderId();
                float score = bookScoreMap.get(book);
                if (score > maxScore) { // 同系列图书仅保留一本，分数为该系列图书用户的最高打分作为该书的打分
                    maxScore = score;
                }
                if (seriesId != 0) {
                    if (serialBookMap.containsKey(seriesId)) {
                        BookInfo seriesBook = serialBookMap.get(seriesId);
                        if (orderId < seriesBook.getOrderId()) {
                            seriesBook = new BookInfo(book, seriesId, orderId,
                                    maxScore);
                            serialBookMap.put(seriesId, seriesBook);
                        }
                    } else {
                        BookInfo seriesBook = new BookInfo(book, seriesId,
                                orderId, maxScore);
                        serialBookMap.put(seriesId, seriesBook);
                    }
                } else {
                    nonSerialBookMap.put(book, score);
                }

            }
        }
        Set<Integer> serialBookSet = serialBookMap.keySet();
        Iterator<Integer> it = serialBookSet.iterator();
        while (it.hasNext()) {
            int seriesId = it.next();
            BookInfo seriesBookInfo = serialBookMap.get(seriesId);
            String bookId = seriesBookInfo.getBookId();
            float score = seriesBookInfo.getScore();
            nonSerialBookMap.put(bookId, score);
        }
        return nonSerialBookMap;

    }

    /*
     * 过滤同作者图书
     */
    private Map<String, Float> filterSameAuthor(Map<String, Float> bookScoreMap) {
        Map<String, Map<String, Float>> authorBooks = new HashMap<String, Map<String, Float>>();
        Map<String, Float> resultMap = new HashMap<String, Float>();
        Set<String> bookSet = bookScoreMap.keySet();
        for (String book : bookSet) {
            if (bookInfoMap.containsKey(book)) {
                BookInfo bookInfo = bookInfoMap.get(book);
                String authorId = bookInfo.getAuthorId();
                float score = bookScoreMap.get(book);
                if (!authorBooks.containsKey(authorId)) {
                    Map<String, Float> bookScore = new HashMap<String, Float>();
                    bookScore.put(book, score);
                    authorBooks.put(authorId, bookScore);
                } else {
                    Map<String, Float> bookScore = authorBooks.get(authorId);
                    bookScore.put(book, score);
                    authorBooks.put(authorId, bookScore);
                }
            }
        }
        Iterator<Map<String, Float>> bookScores = authorBooks.values()
                .iterator();
        while (bookScores.hasNext()) {
            Map<String, Float> bookScore = bookScores.next();
            bookScore = MergeUtils.sortMapByValue(bookScore, sameAuthorCount); // 同作者只取分数高的前两本
            resultMap.putAll(bookScore);
        }
        return resultMap;
    }

    /* add by liyang
     * 获取统一瀑布流结果
     */
    private Set<String> getUnifyBook(String userId)
            throws Exception {
        Set<String> unifyBookSet = new HashSet<String>();

        Get get = new Get(Bytes.toBytes(userId));
        //Result result = unifyRecomTable.get(get);

        Result result = unifyRecomTable.get(get);
        String resultValue = Bytes.toString(result.getValue(
                Bytes.toBytes("cf"), Bytes.toBytes("result")));
        if (resultValue == null || resultValue.isEmpty()) return unifyBookSet;
        else {
            String[] bookScoreList = resultValue.split("\\|");
            for (int i = 0; i < bookScoreList.length; i++) {
                String book = bookScoreList[i].split(",")[0];
                unifyBookSet.add(book);
            }
            return unifyBookSet;
        }
    }

    //add by liyang 过滤统一瀑布流结果
    private Map<String, Float> filterUnifyBooks(Map<String, Float> bookScoreMap, Set<String> unifyBookSet) {
        Iterator<Entry<String, Float>> iterator = bookScoreMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Float> entry = iterator.next();
            String book = entry.getKey();
            if (unifyBookSet.contains(book)) {
                iterator.remove();
                continue;
            }
        }
        return bookScoreMap;
    }

    //获取图书状态
    private Map<String, String> getBookStatus(HTableInterface htable)
            throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        Scan scan = new Scan();
        ResultScanner rs = htable.getScanner(scan);
        for (Result result : rs) {
            String bookId = Bytes.toString(result.getRow());
            String status = Bytes.toString(result.getValue(
                    Bytes.toBytes("cf"), Bytes.toBytes("result")));
            map.put(bookId, status);
        }
        return map;
    }

    //过滤非在架图书
    private Map<String, Float> filterUnavailableBook(Map<String, Float> bookScoreMap, Map<String, String> bookStatusMap) {
        Iterator<Entry<String, Float>> iterator = bookScoreMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Float> entry = iterator.next();
            String book = entry.getKey();
            if (!bookStatusMap.containsKey(book)) continue;
            else if (bookStatusMap.containsKey(book) && bookStatusMap.get(book).equals("13")) continue;
            else {
                iterator.remove();
                continue;
            }
        }
        return bookScoreMap;
    }

    //过滤补白库中历史图书行为、黑名单、实时图书行为以及统一瀑布流结果
    private Map<String, String> bookFilterOne(Map<String, String> bookFilterMap,
                                              Set<String> filterBookSet, Set<String> UnifyBookSet) {
        Iterator<Entry<String, String>> iterator = bookFilterMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            String book = entry.getKey();
            if (filterBookSet.contains(book)) {
                iterator.remove();
                continue;
            }
            if (UnifyBookSet.contains(book)) {
                iterator.remove();
                continue;
            }
        }
        return bookFilterMap;
    }

    //补白库过滤同系列图书
    private Map<String, String> bookFilterTwo(Map<String, String> bookFilterMap) {
        Map<String, String> nonSerialBookMap = new HashMap<String, String>();
        Map<Integer, BookSeriesInfo> serialBookMap = new HashMap<Integer, BookSeriesInfo>();
        Set<String> bookSet = bookFilterMap.keySet();
        for (String book : bookSet) {
            if (bookInfoMap.containsKey(book)) {
                BookInfo bookInfo = bookInfoMap.get(book);
                int seriesId = bookInfo.getSeriesId();
                int orderId = bookInfo.getOrderId();
                String gene = bookFilterMap.get(book);
                if (seriesId != 0) {
                    if (serialBookMap.containsKey(seriesId)) {
                        BookSeriesInfo seriesBook = serialBookMap.get(seriesId);
                        if (orderId < seriesBook.getOrderId()) {
                            seriesBook = new BookSeriesInfo(book, seriesId,
                                    orderId, gene);
                            serialBookMap.put(seriesId, seriesBook);
                        }
                    } else {
                        BookSeriesInfo seriesBook = new BookSeriesInfo(book,
                                seriesId, orderId, gene);
                        serialBookMap.put(seriesId, seriesBook);
                    }
                } else {
                    nonSerialBookMap.put(book, gene);
                }

            }
        }
        Set<Integer> serialBookSet = serialBookMap.keySet();
        Iterator<Integer> it = serialBookSet.iterator();
        while (it.hasNext()) {
            int seriesId = it.next();
            BookSeriesInfo seriesBookInfo = serialBookMap.get(seriesId);
            String bookId = seriesBookInfo.getBookId();
            String gene = seriesBookInfo.getGene();
            nonSerialBookMap.put(bookId, gene);
        }
        return nonSerialBookMap;
    }

    private Map<String, String> getFilterBook(Map<String, String> allBookFilterCopyMap,
                                              Map<String, String> bookFilterMap) {
        Iterator<Entry<String, String>> iterator = allBookFilterCopyMap
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            String bookId = entry.getKey();
            if (bookFilterMap.containsKey(bookId)) {
                iterator.remove();
                continue;
            }
        }
        return allBookFilterCopyMap;
    }

    private Map<String, ArrayList<String>> getGeneBookList(Map<String, String> bookFilterMap) {
        Map<String, ArrayList<String>> geneBookListMap = new HashMap<String, ArrayList<String>>();

        Iterator<Entry<String, String>> iterator = bookFilterMap
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            String bookId = entry.getKey();
            String gene = entry.getValue();

            String[] fields = gene.split(",");
            for (int i = 0; i < fields.length; i++) {
                if (!geneBookListMap.containsKey(fields[i])) {
                    ArrayList<String> bookList = new ArrayList<String>();
                    bookList.add(bookId);
                    geneBookListMap.put(fields[i], bookList);
                } else {
                    ArrayList<String> bookList = geneBookListMap.get(fields[i]);
                    bookList.add(bookId);
                    geneBookListMap.put(fields[i], bookList);
                }
            }
        }
        return geneBookListMap;
    }

    //获取基因-分类结果
    private Map<String, ArrayList<String>> getGeneType(String geneId, HTableInterface htable)
            throws Exception {

        Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

        String[] geneList = geneId.split(",");
        for (int i = 0; i < geneList.length; i++) {
            Get get = new Get(Bytes.toBytes(geneList[i]));
            Result result = htable.get(get);
            String resultValue = Bytes.toString(result.getValue(
                    Bytes.toBytes("cf"), Bytes.toBytes("type")));
            String[] type = resultValue.split(",");
            ArrayList<String> typeList = new ArrayList<String>();
            for (int j = 0; j < type.length; j++) {
                typeList.add(type[j]);
            }
            map.put(geneList[i], typeList);
        }
        return map;
    }

    //过滤分类不属于用户所选基因的图书
    private Map<String, ArrayList<BookGeneClassInfo>> filterAccordGene(Map<String, Float> bookScoreMap,
                                                                       Map<String, String> bookClassMap,
                                                                       Map<String, ArrayList<String>> geneTypeMap) {
        Map<String, ArrayList<BookGeneClassInfo>> bookGeneScoreMap =
                new HashMap<String, ArrayList<BookGeneClassInfo>>();

        Iterator<Entry<String, Float>> iterator = bookScoreMap.entrySet().iterator();
        while (iterator.hasNext()) {
            int count = 0;
            Entry<String, Float> entry = iterator.next();
            String book = entry.getKey();
            float score = entry.getValue();
            if (bookClassMap.containsKey(book)) {
                String cls = bookClassMap.get(book);
                Set<String> geneSet = geneTypeMap.keySet();
                for (String gene : geneSet) {
                    ArrayList<String> typeList = geneTypeMap.get(gene);
                    if (typeList.contains(cls)) {
                        BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(book, cls, gene, score);
                        if (!bookGeneScoreMap.containsKey(gene)) {
                            ArrayList<BookGeneClassInfo> bookGeneClassInfoSet = new ArrayList<BookGeneClassInfo>();
                            bookGeneClassInfoSet.add(bookGeneClassInfo);
                            bookGeneScoreMap.put(gene, bookGeneClassInfoSet);
                        } else {
                            ArrayList<BookGeneClassInfo> bookGeneClassInfoSet = bookGeneScoreMap.get(gene);
                            bookGeneClassInfoSet.add(bookGeneClassInfo);
                            bookGeneScoreMap.put(gene, bookGeneClassInfoSet);
                        }
                    } else {
                        count++;
                        continue;
                    }
                }
                if (count == geneSet.size()) iterator.remove();
            }
        }
        return bookGeneScoreMap;
    }

    //将每个基因下的图书补齐100 / geneNum 本
    private Map<String, ArrayList<BookGeneClassInfo>> getBookPerGene(
            Map<String, ArrayList<BookGeneClassInfo>> bookGeneScoreMap,
            String geneId,
            Map<String, ArrayList<String>> geneBookListMap, Map<String, String> bookClassMap) {
        String[] geneList = geneId.split(",");
        int N = geneList.length;
        Map<String, ArrayList<BookGeneClassInfo>> finalBookMap = new HashMap<String, ArrayList<BookGeneClassInfo>>();
        ArrayList<String> filter = new ArrayList<String>();
        for (int i = 0; i < N; i++) {
            if (bookGeneScoreMap.containsKey(geneList[i])) {
                ArrayList<BookGeneClassInfo> recomBookList = bookGeneScoreMap.get(geneList[i]);
                Collections.sort(recomBookList, new SortByScore());
                if (recomBookList.size() >= (int) Math.ceil((double) topN / N)) {
                    int cnt = 0;
                    ArrayList<BookGeneClassInfo> bookGeneClassInfoSet = new ArrayList<BookGeneClassInfo>();
                    for (int j = 0; j < recomBookList.size(); j++) {
                        if (!filter.contains(recomBookList.get(j).getBook())) {
                            bookGeneClassInfoSet.add(recomBookList.get(j));
                            filter.add(recomBookList.get(j).getBook());
                            if (j == (int) Math.ceil((double) topN / N) + cnt - 1) break;
                        } else {
                            cnt++;
                            continue;
                        }
                    }
                    if (bookGeneClassInfoSet.size() < (int) Math.ceil((double) topN / N)) {
                        ArrayList<String> bookList = geneBookListMap.get(geneList[i]);

                        //for test
                        if (bookList == null) {
                            PrintHelper.print("gene: " + geneList[i]);
                            continue;
                        }

                        int num = Math.min(bookList.size(), (int) Math.ceil((double) topN / N) - bookGeneClassInfoSet.size());
                        for (int k = 0; k < num; k++) {
                            if (!filter.contains(bookList.get(k))) {
                                String cls = bookClassMap.get(bookList.get(k));
                                float score = 0;
                                BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(bookList.get(k), cls, geneList[i], score);
                                bookGeneClassInfoSet.add(bookGeneClassInfo);
                                filter.add(bookList.get(k));
                            } else continue;
                        }
                    }
                    finalBookMap.put(geneList[i], bookGeneClassInfoSet);
                } else {
                    ArrayList<String> bookList = geneBookListMap.get(geneList[i]);
                    ArrayList<BookGeneClassInfo> bookGeneClassInfoSet = new ArrayList<BookGeneClassInfo>();
                    int count = 0;
                    for (int j = 0; j < recomBookList.size(); j++) {
                        if (!filter.contains(recomBookList.get(j).getBook())) {
                            bookGeneClassInfoSet.add(recomBookList.get(j));
                            filter.add(recomBookList.get(j).getBook());
                            count++;
                        } else continue;
                    }

                    //for test
                    if (bookList == null) {
                        PrintHelper.print("gene: " + geneList[i]);
                        continue;
                    }

                    int num = Math.min(bookList.size(), (int) Math.ceil((double) topN / N) - count);
                    for (int k = 0; k < num; k++) {
                        if (!filter.contains(bookList.get(k))) {
                            String cls = bookClassMap.get(bookList.get(k));
                            float score = 0;
                            BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(bookList.get(k), cls, geneList[i], score);
                            bookGeneClassInfoSet.add(bookGeneClassInfo);
                            filter.add(bookList.get(k));
                        } else continue;
                    }
                    finalBookMap.put(geneList[i], bookGeneClassInfoSet);
                }
            } else {
                ArrayList<String> bookList = geneBookListMap.get(geneList[i]);

                //for test
                if (bookList == null) {
                    PrintHelper.print("gene: " + geneList[i]);
                    continue;
                }

                ArrayList<BookGeneClassInfo> bookGeneClassInfos = new ArrayList<BookGeneClassInfo>();
                int num = Math.min(bookList.size(), (int) Math.ceil((double) topN / N));
                for (int k = 0; k < num; k++) {
                    String cls = bookClassMap.get(bookList.get(k));
                    float score = 0;
                    BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(bookList.get(k), cls, geneList[i], score);
                    bookGeneClassInfos.add(bookGeneClassInfo);
                }
                finalBookMap.put(geneList[i], bookGeneClassInfos);
            }
        }
        return finalBookMap;
    }

    //得到top100本图书并得到最终推荐结果
    private ArrayList<BookGeneClassInfo> getTop(
            Map<String, ArrayList<BookGeneClassInfo>> bookGeneScoreMap,
            Map<String, String> bookClassMap, String geneId,
            Map<String, ArrayList<String>> readGeneBookListMap,
            Map<String, ArrayList<String>> geneBookListMap,
            Map<String, String> bookStatusMap) {
        ArrayList<BookGeneClassInfo> combine = new ArrayList<BookGeneClassInfo>();
        ArrayList<BookGeneClassInfo> allRecomList = new ArrayList<BookGeneClassInfo>();
        ArrayList<BookGeneClassInfo> turnOverList = new ArrayList<BookGeneClassInfo>();

        String[] line = geneId.split(",");
        int geneNum = line.length;
        int min = 100;
        Iterator<ArrayList<BookGeneClassInfo>> it = bookGeneScoreMap.values().iterator();
        while (it.hasNext()) {
            ArrayList<BookGeneClassInfo> list = it.next();
            allRecomList.addAll(list);
            min = Math.min(min, list.size());
        }

        it = bookGeneScoreMap.values().iterator();
        for (int i = 0; i < min; i++) {
            while (it.hasNext()) {
                ArrayList<BookGeneClassInfo> list = it.next();
                combine.add(list.get(i));
                if (combine.size() == topN) break;
            }
            if (combine.size() == topN) break;
            it = bookGeneScoreMap.values().iterator();
        }
        int groupNum = combine.size() / 5;
        turnOverList.addAll(combine);

        if (combine.size() < topN) {
            allRecomList.removeAll(combine);
            combine.addAll(allRecomList);
        }

        ArrayList<BookGeneClassInfo> finalCombine = new ArrayList<BookGeneClassInfo>();
        while (!combine.isEmpty()) {
            ArrayList<BookGeneClassInfo> temp = new ArrayList<BookGeneClassInfo>();
            int len = Math.min(combine.size(), geneNum);
            for (int i = 0; i < len; i++) {
                temp.add(combine.get(i));
            }
            combine.removeAll(temp);
            Collections.sort(temp, new SortByScore());
            finalCombine.addAll(temp);
        }

        if (finalCombine.size() < 50) {
            for (int i = 0; i < geneNum; i++) {
                ArrayList<String> list = readGeneBookListMap.get(line[i]);
                if (list == null) continue;
                for (int j = 0; j < list.size(); j++) {
                    if (finalCombine.size() >= 50) break;
                    String cls = bookClassMap.get(list.get(j));
                    float score = 0;
                    BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(list.get(j), cls, line[i], score);
                    finalCombine.add(bookGeneClassInfo);
                }
                if (finalCombine.size() >= 50) break;
            }
        }

        if (finalCombine.size() < 50) {
            Iterator<Entry<String, ArrayList<String>>> iterator = geneBookListMap
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, ArrayList<String>> entry = iterator.next();
                String gene = entry.getKey();
                if (geneId.contains(gene)) continue;
                ArrayList<String> bookList = geneBookListMap.get(gene);
                for (int i = 0; i < bookList.size(); i++) {
                    if (finalCombine.size() >= 50) break;
                    String cls = bookClassMap.get(bookList.get(i));
                    float score = 0;
                    BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(bookList.get(i), cls, gene, score);
                    finalCombine.add(bookGeneClassInfo);
                }
                if (finalCombine.size() >= 50) break;
            }
        }

        if (finalCombine.size() < 50) {
            Iterator<Entry<String, String>> iterator = bookStatusMap
                    .entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, String> entry = iterator.next();
                String book = entry.getKey();
                String status = bookStatusMap.get(book);
                if (status.equals("13")) {
                    String cls = bookClassMap.get(book);
                    float score = 0;
                    BookGeneClassInfo bookGeneClassInfo = new BookGeneClassInfo(book, cls, null, score);
                    finalCombine.add(bookGeneClassInfo);
                } else continue;
                if (finalCombine.size() >= 50) break;
            }
        }

        ArrayList<BookGeneClassInfo> list = new ArrayList<BookGeneClassInfo>();

        Calendar c = Calendar.getInstance();
        int date = c.get(Calendar.DATE);
        if (groupNum == 0) list = finalCombine;
        else {
            int x = date % groupNum;
            if (x == 0) x = groupNum;
            for (int i = (x - 1) * 5; i < turnOverList.size(); i++) {
                list.add(turnOverList.get(i));
            }
            for (int j = 0; j < (x - 1) * 5; j++) {
                list.add(turnOverList.get(j));
            }

            finalCombine.removeAll(turnOverList);
            list.addAll(finalCombine);
        }
        return list;
    }

    /*
     * 补白图书数据
     */
    private Map<String, String> getFillerBook() {
        Map<String, String> bookFilterMap = new HashMap<String, String>();
    	List<Result> resultList = new ArrayList<Result>();
    	resultList = HbaseScannerByTime.getFillerBook(bookFillerTable, 500, 10);
		for (Result result : resultList) {
			String bookId = Bytes.toString(result.getRow());
			String gene = Bytes.toString(result.getValue(Bytes.toBytes("cf"),
					Bytes.toBytes("gene")));
			bookFilterMap.put(bookId, gene);
		}
        
        //Map<String, String> bookFilterMap = new HashMap<String, String>();
        //Scan scan = new Scan();
        //scan.setCaching(100);
        //try {
        //    ResultScanner rs = bookFillerTable.getScanner(scan);
        //    for (Result result : rs) {
        //        String bookId = Bytes.toString(result.getRow());
        //        String gene = Bytes.toString(result.getValue(Bytes.toBytes("cf"),
        //                Bytes.toBytes("gene")));
        //        bookFilterMap.put(bookId, gene);
        //    }
        //} catch (Exception e) {
        //    e.printStackTrace();
        //}

        if (isFirstLoad) { // 是否是第一次加载
            isFirstLoad = false;
            return bookFilterMap;
        }
        return bookFilterMap;
    }

    /*
     * 图书黑名单
     */
    private Vector<String> getBookBlackList() {
        Vector<String> blackBooks = new Vector<String>();
        Scan scan = new Scan();
        scan.setCaching(100);
        try {
            ResultScanner rs = bookBlackListTable.getScanner(scan);
            for (Result result : rs) {
                String bookId = Bytes.toString(result.getRow());
                blackBooks.add(bookId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return blackBooks;
    }

    /*
     * 获取用户实时数据
     */
    private Set<String> getUserRealBehavior(String userId) {
        Set<String> realBebaviorBooks = new HashSet<String>();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(userId + "|"));
        scan.setStopRow(Bytes.toBytes(userId + "|A"));
        scan.setCaching(100);
        try {
            ResultScanner rs = userRealTable.getScanner(scan);
            for (Result result : rs) {
                String rowKey = Bytes.toString(result.getRow()); // uid|bid|pv/read/order
                String[] tmp = rowKey.split("\\|");
                if (tmp.length == 3) {
                    String bid = tmp[1];
                    String type = tmp[2];
                    if (behaviorSet.contains(type)) {
                        realBebaviorBooks.add(bid);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        PrintHelper.print("GeneMergeBolt user: " + userId
                + ", size of user real books:" + realBebaviorBooks.size());
        return realBebaviorBooks;
    }

    /*
     * 获取用户历史图书及黑名单数据
     */
    private Set<String> getUserHisBlackBooks(String userId) {
        Set<String> hisBlackBooks = new HashSet<String>();
        if (userId.length() >= 4) {
            String rowKey = userId.substring(userId.length() - 4,
                    userId.length() - 2)
                    + userId;
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = null;
            try {
                result = userHisTable.get(get);
                if (!result.isEmpty()) {// 有些用户，没有历史。
                    List<Cell> cells = result.listCells();
                    if (cells != null) {
                        for (Cell cell : cells) {
                            hisBlackBooks.add(Bytes.toString(CellUtil
                                    .cloneQualifier(cell)));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        PrintHelper.print("GeneMergeBolt user: " + userId
                + ", size of user his black books:" + hisBlackBooks.size());
        return hisBlackBooks;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}


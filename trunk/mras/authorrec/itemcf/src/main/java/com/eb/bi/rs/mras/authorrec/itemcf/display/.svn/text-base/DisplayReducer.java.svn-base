package com.eb.bi.rs.mras.authorrec.itemcf.display;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable.RecommItemWritable;
import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DisplayReducer
        extends Reducer<Text, RecommItemWritable, Text, Text> {

    private int readRecommNum = 9;
    private int totalRecommNum = 2;
    private int type4RecommNum = 2;
    private Map<String, Double> userPrefScoreMap = new HashMap<String, Double>();

    /**
     * 轮换、展示
     * reduce out: key:msisdn; value:authorid|booid|type|展示位置
     * 说明：目前按照需求，对没有协同过滤推荐的用户启用补白
     * 若以后需要对协同过滤不足的用户也启用补白，修改此代码
     */
    public void reduce(Text key, Iterable<RecommItemWritable> values,
                       Context context) throws IOException, InterruptedException {
        Map<Integer, List<RecommendItem>> recommMap
                = new HashMap<Integer, List<RecommendItem>>();
        Map<Integer, List<RecommendItem>> displayRecommMap
                = new HashMap<Integer, List<RecommendItem>>();
        boolean bNeedFiller = true;
        String msisdn = key.toString();
        for (RecommItemWritable writable : values) {
            RecommendItem item = writable.getItem();
            int type = writable.getType();
            if (type > 0 && type < 5) {
                bNeedFiller = false;
            }
            List<RecommendItem> items = recommMap.get(type);
            if (items == null) {
                items = new ArrayList<RecommendItem>();
            }
            items.add(item.clone());
            recommMap.put(type, items);
        }
        for (Entry<Integer, List<RecommendItem>> entry : recommMap.entrySet()) {
            Collections.sort(entry.getValue());
        }
        //read authors
        List<RecommendItem> readAuthorItems = recommMap.get(0);
        int count = 0;
        if (readAuthorItems != null) {
            int readAddNum = 0;
            for (int i = 0; i < readAuthorItems.size(); i++) {
                RecommendItem temp = readAuthorItems.get(i);
                boolean bAdd = addDisplayItems(msisdn, temp, 0, displayRecommMap);
                if (bAdd) {
                    count++;
                    readAddNum++;
                }
                if (readAddNum == readRecommNum) {
                    break;
                }
            }
        }
        int type4Index = 4;
        int dis = 0;
        if (bNeedFiller) {
            dis = 50;
        }
        type4Index += dis;
        count = recommType4(msisdn, count, type4Index, false, recommMap, displayRecommMap);
        count = recommPrefClass(count, recommMap, displayRecommMap, msisdn, dis, false);
        int leftCount = totalRecommNum - count;
        if (leftCount > 0) {
            count = recommType4(msisdn, count, type4Index, true, recommMap, displayRecommMap);
        }
        leftCount = totalRecommNum - count;
        if (leftCount > 0) {
            recommPrefClass(count, recommMap, displayRecommMap, msisdn, dis, true);
        }
        List<RecommendItem> orderItems = new ArrayList<RecommendItem>();
        List<Integer> orderTypes = ordeRecommendItems(
                dis, displayRecommMap, orderItems);
        for (int i = 0; i < orderItems.size(); i++) {
            RecommendItem oItem = orderItems.get(i);
            String outstr = String.format("%s|%s|%d|%d", oItem.getAuthorId(),
                    oItem.getBookId(), orderTypes.get(i), i + 1);
            context.write(key, new Text(outstr));
        }
    }

    private List<Integer> ordeRecommendItems(int dis,
                                             Map<Integer, List<RecommendItem>> displayRecommMap,
                                             List<RecommendItem> orderItems) {
        List<Integer> orderTypes = new ArrayList<Integer>();
        List<Integer> disPoses = new ArrayList<Integer>();
        disPoses.add(0);
        disPoses.add(dis + 4);
        for (int i = 1; i < 4; i++) {
            disPoses.add(dis + i);
        }
        while (orderItems.size() < totalRecommNum
                && getDisplayMapSize(displayRecommMap) > 0) {
            for (int i = 0; i < disPoses.size(); i++) {
                if (orderItems.size() >= totalRecommNum) {
                    break;
                }
                int idx = disPoses.get(i);
                List<RecommendItem> typeItems = displayRecommMap.get(idx);
                if (typeItems == null || typeItems.size() == 0) {
                    continue;
                }
                RecommendItem item = typeItems.get(0);
                orderItems.add(item);
                orderTypes.add(idx);
                typeItems.remove(0);
            }
        }
        return orderTypes;
    }

    private int getDisplayMapSize(Map<Integer, List<RecommendItem>> displayRecommMap) {
        int count = 0;
        for (Entry<Integer, List<RecommendItem>> entry : displayRecommMap.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            count += entry.getValue().size();
        }
        return count;
    }

    private int recommType4(String msisdn, int count, int type4Index, boolean bSupplement,
                            Map<Integer, List<RecommendItem>> recommMap,
                            Map<Integer, List<RecommendItem>> displayRecommMap) {
        int leftCount = totalRecommNum - count;
        List<RecommendItem> type4AuthorItems = recommMap.get(type4Index);
        int start = 0;
        int fetchNum = type4RecommNum;
        if (bSupplement) {
            start = type4RecommNum;
            fetchNum = leftCount;
        }
        int addNum = 0;
        if (type4AuthorItems != null) {
            for (int i = start; i < type4AuthorItems.size(); i++) {
                RecommendItem temp = type4AuthorItems.get(i);
                boolean bAdd = addDisplayItems(msisdn, temp, type4Index, displayRecommMap);
                if (bAdd) {
                    addNum++;
                    count++;
                }
                if (addNum == fetchNum) {
                    break;
                }
            }
        }
        return count;
    }

    private int recommPrefClass(int count, Map<Integer, List<RecommendItem>> recommMap,
                                Map<Integer, List<RecommendItem>> displayRecommMap, String msisdn,
                                int dis, boolean bSupplement) {
        int leftCount = totalRecommNum - count;
        int start = 0;
        int fetchNum = 0;
        for (int k = 1; k < 4; k++) {
            int prefIdx = k + dis;
            List<RecommendItem> typeAuthorItems = recommMap.get(prefIdx);
            if (typeAuthorItems == null) {
                continue;
            }
            String userClass = String.format("%s|%d", msisdn, k);
            Double prefScore = userPrefScoreMap.get(userClass);
            if (prefScore == null) {
                continue;
            }
            int typeNum = (int) (leftCount * prefScore + 1);
            if (bSupplement) {
                start = typeNum;
                fetchNum = leftCount;
            } else {
                fetchNum = typeNum;
            }
            int addNum = 0;
            for (int i = start; i < typeAuthorItems.size(); i++) {
                RecommendItem temp = typeAuthorItems.get(i);
                boolean bAdd = addDisplayItems(msisdn, temp, prefIdx, displayRecommMap);
                if (bAdd) {
                    addNum++;
                    count++;
                }
                if (addNum == fetchNum) {
                    break;
                }
            }
        }
        return count;
    }

    private boolean addDisplayItems(String msisdn, RecommendItem item, int idx,
                                    Map<Integer, List<RecommendItem>> displayRecommMap) {
        List<RecommendItem> displays = displayRecommMap.get(idx);
        if (displays == null) {
            displays = new ArrayList<RecommendItem>();
        }
        displays.add(item);
        displayRecommMap.put(idx, displays);
        return true;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        readRecommNum = context.getConfiguration().getInt(
                PluginUtil.RECOMM_AUTHOR_NUM_READ, readRecommNum);
        totalRecommNum = context.getConfiguration().getInt(
                PluginUtil.RECOMM_AUTHOR_NUM, totalRecommNum);
        type4RecommNum = context.getConfiguration().getInt(
                PluginUtil.RECOMM_AUTHOR_NUM_TYPE4, type4RecommNum);

        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String prefPath = context.getConfiguration().get(
                PluginUtil.TEMP_TORECOMM_USER_PREF_FILTER_KEY);
        prefPath = execuUtil.getFileName(prefPath);
        //用户偏好
        //格式：msisdn|class1_id|score1|class2_id|score2|class3_id|score3
        for (URI path : localFiles) {
            if (!path.toString().contains(prefPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    int prefIdx = 0;
                    for (int i = 1; i < fields.length; i += 2) {
                        prefIdx++;
                        if (fields[i].equalsIgnoreCase("null")
                                || fields[i].equalsIgnoreCase("\\N")
                                || fields[i + 1].equalsIgnoreCase("null")
                                || fields[i + 1].equalsIgnoreCase("\\N")) {
                            continue;
                        }
                        String temp = String.format("%s|%d", msisdn, prefIdx);
                        double score = Double.parseDouble(fields[i + 1]);
                        userPrefScoreMap.put(temp, score);
                    }
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String strLog = String.format("DisplayReducer set up userPrefScoreMap "
                + "size: %d", userPrefScoreMap.size());
        System.out.println(strLog);
    }
}

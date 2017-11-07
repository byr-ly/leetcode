package com.eb.bi.rs.mras2.booklistrec.booklist_score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/7/11.
 */
public class BookListScoreReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<String>> sheetTypeMap;
    private HashMap<String, ArrayList<String>> sheetTagMap;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double score = 0.0;
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if(line.length < 14) continue;
            String userType = line[1];
            String class1 = line[2];
            String class2 = line[4];
            String class3 = line[6];
            String class1_value = line[3];
            String class2_value = line[5];
            String class3_value = line[7];
            String tag1 = line[8];
            String tag2 = line[10];
            String tag3 = line[12];
            String tag1_value = line[9];
            String tag2_value = line[11];
            String tag3_value = line[13];

            double class1_dValue, class2_dValue, class3_dValue, tag1_dValue, tag2_dValue, tag3_dValue;
            if (class1_value.isEmpty()) class1_dValue = 0.0;
            else class1_dValue = Double.parseDouble(class1_value);
            if (class2_value.isEmpty()) class2_dValue = 0.0;
            else class2_dValue = Double.parseDouble(class2_value);
            if (class3_value.isEmpty()) class3_dValue = 0.0;
            else class3_dValue = Double.parseDouble(class3_value);

            if (tag1_value.isEmpty()) tag1_dValue = 0.0;
            else tag1_dValue = Double.parseDouble(tag1_value);
            if (tag2_value.isEmpty()) tag2_dValue = 0.0;
            else tag2_dValue = Double.parseDouble(tag2_value);
            if (tag3_value.isEmpty()) tag3_dValue = 0.0;
            else tag3_dValue = Double.parseDouble(tag3_value);

            double typeScore = class1_dValue + class2_dValue + class3_dValue;
            double tagScore = tag1_dValue + tag2_dValue + tag3_dValue;

            if (((userType.equals("2") || userType.equals("3")) && typeScore >= 0.5 && tagScore >= 0.3)
                    || (userType.equals("4") && typeScore >= 0.9 && tagScore >= 0.3)) {
                Iterator iter = sheetTypeMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String sheetId = (String) entry.getKey();
                    ArrayList<String> typeList = sheetTypeMap.get(sheetId);
                    ArrayList<String> tagList = sheetTagMap.get(sheetId);

                    if (typeList.contains(class1)) {
                        score += 0.5;
                    }
                    if (typeList.contains(class2)) {
                        score += 0.3;
                    }
                    if (typeList.contains(class3)) {
                        score += 0.2;
                    }
                    if (tagList.contains(tag1) || tagList.contains(tag2) ||
                            tagList.contains(tag3)) {
                        score += 0.2;
                    }
                    context.write(new Text(key.toString() + "|" + sheetId + "|" + score), NullWritable.get());
                    score = 0.0;
                }
            } else if (((userType.equals("2") || userType.equals("3")) && typeScore >= 0.5 && ((tagScore < 0.3) || tag1_value.equals("")))
                    || (userType.equals("4") && typeScore >= 0.9 && ((tagScore < 0.3) || tag1_value.equals("")))) {
                Iterator iter = sheetTypeMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String sheetId = (String) entry.getKey();
                    ArrayList<String> typeList = sheetTypeMap.get(sheetId);
                    ArrayList<String> tagList = sheetTagMap.get(sheetId);

                    if (typeList.contains(class1)) {
                        score += 0.5;
                    }
                    if (typeList.contains(class2)) {
                        score += 0.3;
                    }
                    if (typeList.contains(class3)) {
                        score += 0.2;
                    }
                    if (tagList.contains(tag1) || tagList.contains(tag2)
                            || tagList.contains(tag3)) {
                        score += 0.1;

                    }
                    context.write(new Text(key.toString() + "|" + sheetId + "|" + score), NullWritable.get());
                    score = 0.0;
                }
            } else if (((userType.equals("2") || userType.equals("3")) && typeScore < 0.5 && tagScore >= 0.3)
                    || (userType.equals("4") && typeScore < 0.9 && tagScore >= 0.3)) {
                Iterator iter = sheetTypeMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String sheetId = (String) entry.getKey();
                    ArrayList<String> typeList = sheetTypeMap.get(sheetId);
                    ArrayList<String> tagList = sheetTagMap.get(sheetId);

                    if (typeList.contains(class1) || typeList.contains(class2) || typeList.contains(class3)) {
                        score += 0.3;
                    }
                    if (tagList.contains(tag1) || tagList.contains(tag2)
                            || tagList.contains(tag3)) {
                        score += 0.2;
                    }
                    context.write(new Text(key.toString() + "|" + sheetId + "|" + score), NullWritable.get());
                    score = 0.0;
                }
            } else if (((userType.equals("2") || userType.equals("3")) && typeScore < 0.5 && ((tagScore < 0.3) || tag1_value.equals("")))
                    || (userType.equals("4") && typeScore < 0.9 && ((tagScore < 0.3) || tag1_value.equals("")))) {
                Iterator iter = sheetTypeMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String sheetId = (String) entry.getKey();
                    ArrayList<String> typeList = sheetTypeMap.get(sheetId);
                    ArrayList<String> tagList = sheetTagMap.get(sheetId);

                    if(key.toString().equals("15088602110") && sheetId.equals("38152")) {
                        System.out.println(tagList);
                    }

                    if (typeList.contains(class1) || typeList.contains(class2) || typeList.contains(class3)) {
                        score += 0.3;
                    }

                    if (tagList.contains(tag1) || tagList.contains(tag2)
                            || tagList.contains(tag3)) {
                        score += 0.1;
                    }
                    context.write(new Text(key.toString() + "|" + sheetId + "|" + score), NullWritable.get());
                    score = 0.0;
                }
            }
        }


    }

    protected void setup(Context context) throws IOException, InterruptedException {

        sheetTypeMap = new HashMap<String, ArrayList<String>>();
        sheetTagMap = new HashMap<String, ArrayList<String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
//        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        URI[] localFiles = context.getCacheFiles();
        for (int i = 0; i < localFiles.length; ++i) {
            String line;
            BufferedReader in = null;
            try {
                /*DistributedCache修改点*/
                FileSystem fs = FileSystem.get(localFiles[i], conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i])),"UTF-8"));

                if (localFiles[i].toString().contains("desc")) {
                    while ((line = in.readLine()) != null) {////书单 | 书单名 | 书单描述 | 包含分类 | 包含标签
                        String[] fields = line.split("\\|");
                        if (fields.length < 5) {
                            continue;
                        }
                        String sheetId = fields[0];
                        String[] type = fields[3].split(",");
                        String[] tag = fields[4].split(",");
                        ArrayList<String> typeList = new ArrayList<String>();
                        ArrayList<String> tagList = new ArrayList<String>();
                        for (int k = 0; k < type.length; k++) {
                            typeList.add(type[k]);
                        }
                        for (int j = 0; j < tag.length; j++) {
                            tagList.add(tag[j]);
                        }
                        sheetTypeMap.put(sheetId, typeList);
                        sheetTagMap.put(sheetId, tagList);
                    }
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}


package com.eb.bi.rs.mras2.booklistrec.booklist_choose;


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
import java.util.*;

/**
 * Created by liyang on 2016/4/27.
 */
public class BookListChooseReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<String>> sheetBookMap;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<String> readBookList = new ArrayList<String>();
        HashMap<String, Double> sheetScoreMap = new HashMap<String, Double>();
        HashMap<String, ArrayList<String>> depthMap = new HashMap<String, ArrayList<String>>();

        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (line.length == 2) {
                if (line[1].equals("0") || line[1].equals("1") || line[1].equals("2") || line[1].equals("3")) {
                    String book = line[0];
                    if (!readBookList.contains(book) && !line[1].equals("0")) readBookList.add(book);
                    if (!depthMap.containsKey(line[1])) {
                        ArrayList<String> depthBookList = new ArrayList<String>();
                        depthBookList.add(book);
                        depthMap.put(line[1], depthBookList);
                    } else {
                        ArrayList<String> depthBookList = depthMap.get(line[1]);
                        depthBookList.add(book);
                        depthMap.put(line[1], depthBookList);
                    }
                } else {
                    String sheetId = line[0];
                    double score = Double.parseDouble(line[1]);
                    sheetScoreMap.put(sheetId, score);
                }
            }
        }

        //遍历sheetScoreMap，对每一个书单进行评分
        ArrayList<SheetScore> sheetList = new ArrayList<SheetScore>();
        Iterator iter = sheetScoreMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String sheet = (String) entry.getKey();

            double score = sheetScoreMap.get(sheet);
            if (sheetBookMap.containsKey(sheet)) {
                //获取每个书单的图书信息
                ArrayList<String> bookList = sheetBookMap.get(sheet);
                if (depthMap.containsKey("0") || depthMap.containsKey("1") || depthMap.containsKey("2") || depthMap.containsKey("3")) {
                    ArrayList<String> overDeepList = depthMap.get("1");
                    ArrayList<String> deepList = depthMap.get("2");
                    ArrayList<String> lightList = depthMap.get("3");
                    ArrayList<String> overDeepListCopy = new ArrayList<String>();
                    if (overDeepList != null) overDeepListCopy.addAll(overDeepList);
                    ArrayList<String> deepListCopy = new ArrayList<String>();
                    if (deepList != null) deepListCopy.addAll(deepList);
                    ArrayList<String> lightListCopy = new ArrayList<String>();
                    if (lightList != null) lightListCopy.addAll(lightList);

                    overDeepListCopy.retainAll(bookList);
                    deepListCopy.retainAll(bookList);
                    lightListCopy.retainAll(bookList);
                    if (overDeepListCopy.size() + deepListCopy.size() == 1) {
                        score += 0.5;
                        SheetScore sheetScore = new SheetScore(sheet, score);
                        sheetList.add(sheetScore);
                    } else if (overDeepListCopy.size() + deepListCopy.size() >= 2) {
                        score += 1.0;
                        SheetScore sheetScore = new SheetScore(sheet, score);
                        sheetList.add(sheetScore);
                    } else if (overDeepListCopy.size() + deepListCopy.size() == 0 && lightListCopy.size() != 0) {
                        score += 0.3;
                        SheetScore sheetScore = new SheetScore(sheet, score);
                        sheetList.add(sheetScore);
                    } else {
                        SheetScore sheetScore = new SheetScore(sheet, score);
                        sheetList.add(sheetScore);
                    }
                }
            }
        }

        Collections.sort(sheetList, new SortByScore());

        int count = 0;
        for (int i = 0; i < sheetList.size(); i++) {
            if (count == 5) break;
            ArrayList<String> noReadList = new ArrayList<String>();
            noReadList.addAll(readBookList);

            String sheetid = sheetList.get(i).sheetId;
            Double score = sheetList.get(i).score;
            if (!sheetBookMap.containsKey(sheetid)) continue;
            else {
                ArrayList<String> hasReadList = sheetBookMap.get(sheetid);
                ArrayList<String> hasReadListCopy = new ArrayList<String>();
                hasReadListCopy.addAll(hasReadList);
                noReadList.retainAll(hasReadListCopy);
                hasReadListCopy.removeAll(noReadList);

                if (hasReadListCopy.size() < 4) continue;
                else if (hasReadListCopy.size() == 4) {
                    if (noReadList.size() == 0) continue;
                    Random rand = new Random();
                    int randNum = rand.nextInt(noReadList.size());
                    hasReadListCopy.add(noReadList.get(randNum));
                    StringBuffer result = new StringBuffer();
                    for (int j = 0; j < hasReadListCopy.size(); j++) {
                        result.append(hasReadListCopy.get(j) + ",");
                    }
                    result.deleteCharAt(result.length() - 1);
                    count++;
                    context.write(new Text(key.toString() + "|" + sheetid + "|" + result.toString() + "|" + score.toString()), NullWritable.get());
                } else {
                    ArrayList<String> resultList = new ArrayList<String>();
                    Random rand = new Random();
                    for (int k = 0; k < 5; k++) {
                        int randNum = rand.nextInt(hasReadListCopy.size());
                        String book = hasReadListCopy.get(randNum);
                        resultList.add(book);
                        hasReadListCopy.remove(book);
                    }
                    StringBuffer result = new StringBuffer();
                    for (int j = 0; j < resultList.size(); j++) {
                        result.append(resultList.get(j) + ",");
                    }
                    result.deleteCharAt(result.length() - 1);
                    count++;
                    context.write(new Text(key.toString() + "|" + sheetid + "|" + result.toString() + "|" + score.toString()), NullWritable.get());
                }
            }
        }
    }


    protected void setup(Context context) throws IOException, InterruptedException {

        sheetBookMap = new HashMap<String, ArrayList<String>>();

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

                if (localFiles[i].toString().contains("book")) {
                    while ((line = in.readLine()) != null) {//书单 | 包含图书
                        String[] fields = line.split("\\|");
                        if (fields.length < 2) {
                            continue;
                        }
                        String sheetId = fields[0];
                        String[] bookId = fields[1].split(",");

                        if (!sheetBookMap.containsKey(sheetId)) {
                            ArrayList<String> bookList = new ArrayList<String>();
                            for (int j = 0; j < bookId.length; j++) {
                                if (bookList.contains(bookId[j])) continue;
                                bookList.add(bookId[j]);
                            }
                            sheetBookMap.put(sheetId, bookList);
                        } else {
                            ArrayList<String> bookList = sheetBookMap.get(sheetId);
                            for (int j = 0; j < bookId.length; j++) {
                                if (bookList.contains(bookId[j])) continue;
                                bookList.add(bookId[j]);
                            }
                            sheetBookMap.put(sheetId, bookList);
                        }
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


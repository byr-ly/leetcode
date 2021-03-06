package com.eb.bi.rs.mras2.booklistrec.booklist_filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by liyang on 2016/4/28.
 */
public class BookListFilterReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, String> sheetDescMap;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<String> resultList = new ArrayList<String>();

        for (Text val : values) {
            resultList.add(val.toString());
        }

        StringBuffer result = new StringBuffer();
        if (resultList.size() <= 5) {
            for (int i = 0; i < resultList.size(); i++) {
                String recom = resultList.get(i);
                String[] fields = recom.split("\\|");
                String sheet = fields[0];
                String book = fields[1];
                String score = fields[2];
                String sheetJoin = "";
                if (sheetDescMap.containsKey(sheet)) {
                    sheetJoin = sheetDescMap.get(sheet);
                }
                result.append(sheet + "|" + sheetJoin + "|" + book + "|" + score + "&");
            }
        } else {
            for (int i = 0; i < 5; i++) {
                String recom = resultList.get(i);
                String[] fields = recom.split("\\|");
                String sheet = fields[0];
                String book = fields[1];
                String score = fields[2];
                String sheetJoin = "";
                if (sheetDescMap.containsKey(sheet)) {
                    sheetJoin = sheetDescMap.get(sheet);
                }
                result.append(sheet + "|" + sheetJoin + "|" + book + "|" + score + "&");
            }
        }
        result.deleteCharAt(result.length() - 1);
        context.write(new Text(key.toString()), new Text(result.toString()));
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        sheetDescMap = new HashMap<String, String>();

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
                    while ((line = in.readLine()) != null) {//书单 | 书单名 | 书单描述 | 包含分类 | 包含标签
                        String[] fields = line.split("\\|");
                        if (fields.length < 5) {
                            continue;
                        }
                        String sheetId = fields[0];
                        String sheetName = fields[1];
                        String sheetDesc = fields[2];
                        String sheetJoin = sheetName + "|" + sheetDesc;
                        sheetDescMap.put(sheetId, sheetJoin);
                    }
                    System.out.println("书单描述列表加载成功 " + sheetDescMap.size());
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}

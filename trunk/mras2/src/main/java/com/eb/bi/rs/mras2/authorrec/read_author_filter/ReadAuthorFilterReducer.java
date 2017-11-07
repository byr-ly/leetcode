package com.eb.bi.rs.mras2.authorrec.read_author_filter;

import com.eb.bi.rs.mras2.authorrec.pre_author_filter.Author;
import com.eb.bi.rs.mras2.authorrec.pre_author_filter.SortByScore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by liyang on 2016/3/15.
 */
public class ReadAuthorFilterReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<String>> authorBookMap;
    private ArrayList<String> bookList;
    private HashMap<String, HashMap<String, String>> userHistory;
    private HashMap<String, String> bookDepthMap;
    private ArrayList<Author> authorList;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        authorList = new ArrayList<Author>();
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (Double.parseDouble(line[1]) < 4.0) {
                continue;
            } else {
                String writer = line[0];
                if (authorBookMap.containsKey(writer)) {
                    bookList = authorBookMap.get(writer);
                } else {
                    continue;
                }
                if (userHistory.containsKey(key.toString())) {
                    bookDepthMap = userHistory.get(key.toString());
                } else {
                    continue;
                }
                for (int i = 0; i < bookList.size(); i++) {
                    if (bookDepthMap.containsKey(bookList.get(i)) && bookDepthMap.get(bookList.get(i)).equals("0")) {
                        Author author = new Author(writer, Double.parseDouble(line[1]));
                        authorList.add(author);
                    } else {
                        continue;
                    }
                }
            }
        }
        Collections.sort(authorList, new SortByScore());
        StringBuffer readAuthor = new StringBuffer();
        StringBuffer readScore = new StringBuffer();
        if (authorList.size() >= 10) {
            for (int j = 0; j < 10; j++) {
                readAuthor.append(authorList.get(j).name + "|");
                readScore.append(authorList.get(j).score + "|");
            }
            context.write(new Text(key + "|" + readAuthor + readScore + "0"), NullWritable.get());
        } else {
            for (int j = 0; j < authorList.size(); j++) {
                readAuthor.append(authorList.get(j).name + "|");
                readScore.append(authorList.get(j).score + "|");
            }
            context.write(new Text(key + "|" + readAuthor + readScore + "0"), NullWritable.get());
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        authorBookMap = new HashMap<String, ArrayList<String>>();
        userHistory = new HashMap<String, HashMap<String, String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("book")) {
                //图书 | 作家
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    }
                    String book = fields[0];
                    String writer = fields[1];
                    if (!authorBookMap.containsKey(writer)) {
                        bookList = new ArrayList<String>();
                        bookList.add(book);
                        authorBookMap.put(writer, bookList);
                    } else {
                        bookList.add(book);
                    }
                }
                br.close();
                System.out.println("图书信息表加载成功 " + authorBookMap.size());
            } else {
                //用户 | 图书 | 阅读深度
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    }
                    String user = fields[0];
                    String book = fields[1];
                    String depth = fields[2];
                    if (!userHistory.containsKey(user)) {
                        bookDepthMap = new HashMap<String, String>();
                        bookDepthMap.put(book, depth);
                        userHistory.put(user, bookDepthMap);
                    } else {
                        bookDepthMap.put(book, depth);
                    }
                }
                br.close();
                System.out.println("智能推荐基础数据加载成功 " + userHistory.size());
            }
        }
    }
}

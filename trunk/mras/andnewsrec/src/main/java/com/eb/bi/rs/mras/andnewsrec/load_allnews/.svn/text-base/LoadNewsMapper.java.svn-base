package com.eb.bi.rs.mras.andnewsrec.load_allnews;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by LiMingji on 2016/3/18.
 */
public class LoadNewsMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\u0001", -1);
        if (fields.length < 4) {
            System.out.println("LoadNewsMapper: Bad Line: " + value.toString());
            return;
        }
        String newsID = fields[0];
        String newsTitle = fields[2].replaceAll("[\\pP￥$^+~|<>\r\n]", " ");
        String newsContent = fields[3].replaceAll("[\\pP￥$^+~|<>\r\n]", " ");

        int totalWordNums = 0;
        Map<String, NewsWord> map = SegMore.segMore("N", newsTitle, newsContent);

        StringBuffer values = new StringBuffer("");
        Iterator<String> it = map.keySet().iterator();
        while (it.hasNext()) {
            String word = it.next();
            NewsWord newsWord = map.get(word);

            values.append(newsWord.word);
            values.append(",");
            values.append(newsWord.times);
            values.append("|");
            totalWordNums += newsWord.times;
        }
        context.write(new Text(newsID + "|" + totalWordNums), new Text(values.toString()));
    }
}
package com.eb.bi.rs.mras2.andnewsrec.get_keyword_news;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 2016/4/12.
 */
public class GetKeyWordNewsMapper extends Mapper<Object, Text, Text, Text> {
    /***
     * @param key
     * @param value
     *
     * value的格式为：
     *       newsID|classID word,weight|word,weight....
     * 输出数据为：
     *       KEY:word|classID
     *       VALUE:newsID|weight
     *
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().trim().isEmpty() || value.toString().split("\t").length < 2) {
            System.out.println("GetKeyWordNewsMapper bad line : " + value.toString());
            return;
        }
        String newsKey = value.toString().split("\t")[0];
        String wordsWeight = value.toString().split("\t")[1];

//        String[] keyFields = newsKey.split("\\|");
//        if (keyFields.length < 2) {
//            System.out.println("GetKeyWordNewsMapper bad line : newKey " + newsKey.toString() + " " + keyFields.length);
//            return;
//        }
        String newsID =newsKey;
        //String classID = keyFields[1];

        String[] valuesFields = wordsWeight.split("\\|");
        for (String perWordWeight : valuesFields) {
            if(perWordWeight.split(",").length < 2){
                continue;
            }
            String word = perWordWeight.split(",")[0];
            String weight = perWordWeight.split(",")[1];
            if (word.trim().isEmpty()) {
                continue;
            }
            context.write(new Text(word), new Text(newsID + "," + weight));
        }
    }
}
